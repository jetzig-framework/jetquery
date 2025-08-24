const std = @import("std");
const ArrayListManaged = std.array_list.Managed;

const jetquery = @import("jetquery");
const jetcommon = @import("jetcommon");

const util = @import("util.zig");

pub const ReflectOptions = struct {
    header: []const u8 = "",
    import_jetquery: []const u8 =
        \\@import("jetquery")
    ,
};

pub fn Reflect(adapter_name: jetquery.adapters.Name, Schema: type) type {
    return struct {
        const Self = @This();
        const AdaptedRepo = jetquery.Repo(adapter_name, Schema);
        allocator: std.mem.Allocator,
        repo: *AdaptedRepo,
        options: ReflectOptions,

        pub fn init(allocator: std.mem.Allocator, repo: *AdaptedRepo, options: ReflectOptions) Self {
            return .{ .repo = repo, .allocator = allocator, .options = options };
        }

        pub fn generateSchema(self: Self) ![]const u8 {
            var arena = std.heap.ArenaAllocator.init(self.allocator);
            defer arena.deinit();

            const allocator = arena.allocator();

            var buf = ArrayListManaged(u8).init(allocator);
            defer buf.deinit();

            const writer = buf.writer();

            try writer.print(
                \\{s}
                \\const jetquery = {s};
                \\
            , .{ self.options.header, self.options.import_jetquery });

            const reflection = try self.repo.adapter.reflect(allocator, self.repo);
            const map = try reflection.tableMap(allocator);
            var written = std.BufSet.init(allocator);

            // Write tables already defined in the schema first to preserve schema order if
            // edited by user ....
            inline for (comptime std.meta.declarations(Schema)) |decl| {
                if (map.get(@field(Schema, decl.name).name)) |table| {
                    try writeModel(allocator, Schema, reflection, table, writer);
                    try written.insert(table.name);
                }
            }

            // ... then write any remaining tables.
            for (reflection.tables) |table| {
                if (written.contains(table.name)) continue;
                try writeModel(allocator, Schema, reflection, table, writer);
            }

            return try self.allocator.dupe(u8, try jetcommon.fmt.zig(
                allocator,
                buf.items,
                "Found errors in generated schema.",
            ));
        }
    };
}

fn writeModel(
    allocator: std.mem.Allocator,
    comptime schema: type,
    reflection: jetquery.Reflection,
    table: jetquery.Reflection.TableInfo,
    writer: anytype,
) !void {
    const model_name = try translateTableName(allocator, schema, table.name);
    try writer.print(
        \\
        \\pub const {s} = jetquery.Model(
        \\@This(),
        \\"{s}",
        \\struct {{
        \\
    , .{ model_name, try util.zigEscape(allocator, .string, table.name) });

    for (reflection.columns) |column| {
        if (!std.mem.eql(u8, column.table, table.name)) continue;
        try writer.print(
            \\{s}: {s},
            \\
        ,
            .{ try util.zigEscape(allocator, .id, column.name), column.zigType() },
        );
    }

    try writer.print(
        \\}},
        \\{s}
        \\);
        \\
    , .{try stringifyOptions(allocator, schema, reflection, table.name)});
}

fn stringifyOptions(
    allocator: std.mem.Allocator,
    comptime schema: type,
    reflection: jetquery.Reflection,
    table_name: []const u8,
) ![]const u8 {
    inline for (comptime std.meta.declarations(schema)) |decl| {
        const table = @field(schema, decl.name);
        if (std.mem.eql(u8, table.name, table_name)) {
            return try stringifyModelOptions(allocator, table);
        }
    }

    var relations_buf = ArrayListManaged(u8).init(allocator);
    defer relations_buf.deinit();

    const relations_writer = relations_buf.writer();
    // TODO: If there are multiple foreign keys to the same table, we don't try to guess the
    // relation, e.g. `foo_id` and `other_foo_id`. We could guess `foo` and `other_foo` for
    // `belongsTo` relations, but `hasMany` would require pluralization which is a bit awkward
    // so, for now, we just use the table name (singularized for `belongsTo`) and users can
    // adjust - we can do pluralization later but it starts to feel a bit overengineered for just
    // generating an initial schema when a new table is discovered. Users are expected to do at
    // least a bit of work here.
    for (reflection.foreign_keys) |foreign_key| {
        // belongsTo
        if (std.mem.eql(u8, foreign_key.table, table_name)) {
            const model_name = try util.modelize(
                allocator,
                foreign_key.foreign_table,
            );
            const singularized = try util.singularize(allocator, foreign_key.foreign_table);
            try relations_writer.print(
                \\.{s} = jetquery.belongsTo(.{s}, .{{
            ,
                .{ singularized, model_name },
            );
            const expected_foreign_key = try std.mem.concat(
                allocator,
                u8,
                &.{ singularized, "_id" },
            );
            if (!std.mem.eql(u8, foreign_key.foreign_column, "id")) {
                try relations_writer.print(
                    \\.primary_key = "{s}",
                , .{foreign_key.foreign_column});
            }
            if (!std.mem.eql(u8, foreign_key.column, expected_foreign_key)) {
                try relations_writer.print(
                    \\.foreign_key = "{s}",
                , .{foreign_key.column});
            }
            try relations_writer.writeAll("}),");
        }

        // hasMany
        if (std.mem.eql(u8, foreign_key.foreign_table, table_name)) {
            const model_name = try util.modelize(
                allocator,
                foreign_key.table,
            );
            const singularized = try util.singularize(allocator, foreign_key.foreign_table);
            try relations_writer.print(
                \\.{s} = jetquery.hasMany(.{s}, .{{
            ,
                .{ foreign_key.table, model_name },
            );
            if (!std.mem.eql(u8, foreign_key.foreign_column, "id")) {
                try relations_writer.print(
                    \\.primary_key = "{s}",
                , .{foreign_key.foreign_column});
            }
            const expected_foreign_key = try std.mem.concat(
                allocator,
                u8,
                &.{ singularized, "_id" },
            );
            if (!std.mem.eql(u8, foreign_key.column, expected_foreign_key)) {
                try relations_writer.print(
                    \\.foreign_key = "{s}",
                , .{foreign_key.column});
            }
            try relations_writer.writeAll("}),");
        }
    }

    if (relations_buf.items.len == 0) return ".{}";

    return try std.fmt.allocPrint(allocator,
        \\.{{ .relations = .{{ {s} }}, }},
    , .{relations_buf.items});
}

fn stringifyModelOptions(allocator: std.mem.Allocator, model: type) ![]const u8 {
    var buf = ArrayListManaged(u8).init(allocator);
    const writer = buf.writer();

    if (comptime hasDefaultOptions(model)) return ".{}";

    try writer.print(
        \\.{{
        \\
    , .{});

    if (comptime !isDefaultPrimaryKey(model)) {
        try writer.print(
            \\.primary_key = "{s}",
            \\
        , .{try util.zigEscape(allocator, .string, model.primary_key)});
    }

    const relation_fields = std.meta.fields(@TypeOf(model.relations));

    if (relation_fields.len > 0) {
        try writer.print(
            \\.relations = .{{
            \\
        , .{});
        inline for (relation_fields) |field| {
            const relation = @field(model.relations, field.name);
            try writer.print(
                \\.{s} = {s}(.{s}, {s}),
                \\
            , .{
                try util.zigEscape(allocator, .id, field.name),
                switch (relation.relation_type) {
                    .belongs_to => "jetquery.belongsTo",
                    .has_many => "jetquery.hasMany",
                },
                try util.zigEscape(allocator, .id, relation.relation_model_name),
                try stringifyRelationOptions(allocator, relation),
            });
        }

        try writer.print(
            \\}},
            \\
        , .{});
    }

    try writer.print(
        \\}},
        \\
    , .{});

    return try buf.toOwnedSlice();
}

fn stringifyRelationOptions(allocator: std.mem.Allocator, comptime relation: type) ![]const u8 {
    if (comptime relation.options.primary_key == null and
        relation.options.foreign_key == null) return ".{}";

    var buf = ArrayListManaged(u8).init(allocator);
    const writer = buf.writer();

    try writer.print(".{{", .{});
    if (comptime relation.options.primary_key != null) {
        try writer.print(
            \\.primary_key = "{s}"{s}
            \\
        , .{
            try util.zigEscape(allocator, .string, relation.options.primary_key.?),
            if (comptime relation.options.foreign_key != null) ", " else "",
        });
    }

    if (comptime relation.options.foreign_key != null) {
        try writer.print(
            \\.foreign_key = "{s}"
            \\
        , .{
            try util.zigEscape(allocator, .string, relation.options.foreign_key.?),
        });
    }
    try writer.print("}}", .{});

    return try buf.toOwnedSlice();
}

fn hasDefaultOptions(comptime model: type) bool {
    comptime {
        if (isDefaultPrimaryKey(model)) return false;
        if (std.meta.fields(@TypeOf(model.relations)).len > 0) return false;

        return true;
    }
}

fn isDefaultPrimaryKey(comptime model: type) bool {
    comptime {
        return std.mem.eql(u8, model.primary_key, "id");
    }
}

fn isDefaultForeignKey(comptime model: type) bool {
    comptime {
        return std.mem.eql(u8, model.foreign_key, "_id");
    }
}

fn translateTableName(
    allocator: std.mem.Allocator,
    comptime schema: type,
    name: []const u8,
) ![]const u8 {
    // First try finding an existing model in the current schema.
    inline for (comptime std.meta.declarations(schema)) |decl| {
        if (std.mem.eql(u8, @field(schema, decl.name).name, name)) return decl.name;
    }

    // Then try translating the table name into a model name. This is the only time we do any
    // magic with plural nouns etc. - if the user modifies the default generated value, we use
    // that value going forward as it will now be in the schema.
    var it = std.mem.tokenizeScalar(u8, name, '_');
    var buf = ArrayListManaged([]const u8).init(allocator);
    while (it.next()) |token| {
        var dup = try allocator.dupe(u8, token);
        dup[0] = std.ascii.toUpper(dup[0]);
        try buf.append(dup);
    }
    return try util.singularize(allocator, try std.mem.join(allocator, "", buf.items));
}

test "reflect" {
    var admin_repo = try jetquery.Repo(.postgresql, void).init(
        std.testing.allocator,
        .{
            .adapter = .{
                .database = "postgres",
                .username = "postgres",
                .hostname = "127.0.0.1",
                .password = "password",
                .port = 5432,
            },
        },
    );
    defer admin_repo.deinit();
    try admin_repo.dropDatabase("reflection_test", .{ .if_exists = true });
    try admin_repo.createDatabase("reflection_test", .{});

    const Schema = struct {
        pub const Human = jetquery.Model(
            @This(),
            "humans",
            struct { id: i32, name: []const u8 },
            .{
                .relations = .{
                    .cats = jetquery.relation.hasMany(.Cat, .{ .foreign_key = "custom_foreign_key" }),
                },
            },
        );

        pub const Cat = jetquery.Model(
            @This(),
            "cats",
            struct { id: i32, name: []const u8, human_id: i32, paws: i32 },
            .{
                .relations = .{
                    .human = jetquery.relation.belongsTo(.Human, .{}),
                },
                .primary_key = "custom_primary_key",
            },
        );
    };

    var repo = try jetquery.Repo(.postgresql, Schema).init(
        std.testing.allocator,
        .{
            .adapter = .{
                .database = "reflection_test",
                .username = "postgres",
                .hostname = "127.0.0.1",
                .password = "password",
                .port = 5432,
            },
        },
    );
    defer repo.deinit();

    try repo.createTable("humans", &.{
        jetquery.schema.table.primaryKey("id", .{}),
        jetquery.schema.table.column("name", .string, .{}),
        jetquery.schema.table.timestamps(.{}),
    }, .{});
    try repo.createTable("cats", &.{
        jetquery.schema.table.primaryKey("id", .{}),
        jetquery.schema.table.column("name", .string, .{}),
        jetquery.schema.table.column(
            "human_id",
            .integer,
            .{ .optional = true, .reference = .{ "humans", "id" } },
        ),
        jetquery.schema.table.timestamps(.{}),
    }, .{});
    try repo.createTable("kennels", &.{
        jetquery.schema.table.primaryKey("id", .{}),
        jetquery.schema.table.column("name", .string, .{}),
        jetquery.schema.table.timestamps(.{}),
    }, .{});
    try repo.createTable("dogs", &.{
        jetquery.schema.table.primaryKey("id", .{ .type = .bigint }),
        jetquery.schema.table.column(
            "kennel_id",
            .integer,
            .{ .reference = .{ "kennels", "id" } },
        ),
        jetquery.schema.table.column(
            "other_kennel_id",
            .integer,
            .{ .reference = .{ "kennels", "id" } },
        ),
        jetquery.schema.table.column("name", .string, .{}),
        jetquery.schema.table.column("is_woofy", .boolean, .{}),
        jetquery.schema.table.column("description", .text, .{}),
        jetquery.schema.table.column("bark_rating", .float, .{ .optional = true }),
        jetquery.schema.table.column("food_budget", .decimal, .{ .optional = true }),
        jetquery.schema.table.column("paws", .smallint, .{}),
        jetquery.schema.table.column("teeth", .bigint, .{}),
        jetquery.schema.table.column("more_precise_bark_rating", .double_precision, .{}),
        jetquery.schema.table.timestamps(.{}),
    }, .{});

    const reflect = Reflect(.postgresql, Schema).init(std.testing.allocator, &repo, .{});
    const schema = try reflect.generateSchema();
    defer std.testing.allocator.free(schema);
    try std.testing.expectEqualStrings(
        \\const jetquery = @import("jetquery");
        \\
        \\pub const Human = jetquery.Model(
        \\    @This(),
        \\    "humans",
        \\    struct {
        \\        id: i32,
        \\        name: []const u8,
        \\        created_at: jetquery.DateTime,
        \\        updated_at: jetquery.DateTime,
        \\    },
        \\    .{
        \\        .relations = .{
        \\            .cats = jetquery.hasMany(.Cat, .{ .foreign_key = "custom_foreign_key" }),
        \\        },
        \\    },
        \\);
        \\
        \\pub const Cat = jetquery.Model(
        \\    @This(),
        \\    "cats",
        \\    struct {
        \\        id: i32,
        \\        name: []const u8,
        \\        human_id: ?i32,
        \\        created_at: jetquery.DateTime,
        \\        updated_at: jetquery.DateTime,
        \\    },
        \\    .{
        \\        .primary_key = "custom_primary_key",
        \\        .relations = .{
        \\            .human = jetquery.belongsTo(.Human, .{}),
        \\        },
        \\    },
        \\);
        \\
        \\pub const Dog = jetquery.Model(
        \\    @This(),
        \\    "dogs",
        \\    struct {
        \\        id: i64,
        \\        kennel_id: i32,
        \\        other_kennel_id: i32,
        \\        name: []const u8,
        \\        is_woofy: bool,
        \\        description: []const u8,
        \\        bark_rating: ?f32,
        \\        food_budget: ?[]const u8,
        \\        paws: i16,
        \\        teeth: i64,
        \\        more_precise_bark_rating: f64,
        \\        created_at: jetquery.DateTime,
        \\        updated_at: jetquery.DateTime,
        \\    },
        \\    .{
        \\        .relations = .{
        \\            .kennel = jetquery.belongsTo(.Kennel, .{}),
        \\            .kennel = jetquery.belongsTo(.Kennel, .{
        \\                .foreign_key = "other_kennel_id",
        \\            }),
        \\        },
        \\    },
        \\);
        \\
        \\pub const Kennel = jetquery.Model(
        \\    @This(),
        \\    "kennels",
        \\    struct {
        \\        id: i32,
        \\        name: []const u8,
        \\        created_at: jetquery.DateTime,
        \\        updated_at: jetquery.DateTime,
        \\    },
        \\    .{
        \\        .relations = .{
        \\            .dogs = jetquery.hasMany(.Dog, .{}),
        \\            .dogs = jetquery.hasMany(.Dog, .{
        \\                .foreign_key = "other_kennel_id",
        \\            }),
        \\        },
        \\    },
        \\);
        \\
    , schema);
}
