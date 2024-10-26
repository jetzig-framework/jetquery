const std = @import("std");

const jetquery = @import("jetquery");

const util = @import("util.zig");

const Reflect = @This();

allocator: std.mem.Allocator,
repo: *jetquery.Repo,

pub fn init(allocator: std.mem.Allocator, repo: *jetquery.Repo) Reflect {
    return .{ .repo = repo, .allocator = allocator };
}

pub fn generateSchema(self: Reflect, comptime schema: type) ![]const u8 {
    var arena = std.heap.ArenaAllocator.init(self.allocator);
    defer arena.deinit();

    const allocator = arena.allocator();

    var buf = std.ArrayList(u8).init(allocator);
    defer buf.deinit();

    const writer = buf.writer();

    try writer.print(
        \\const jetquery = @import("jetquery");
        \\
    , .{});

    const reflection = try self.repo.adapter.reflect(allocator, self.repo);
    const map = try reflection.tableMap(allocator);
    var written = std.BufSet.init(allocator);

    // Write tables already defined in the schema first ...
    inline for (comptime std.meta.declarations(schema)) |decl| {
        if (map.get(@field(schema, decl.name).name)) |table| {
            try writeTable(allocator, schema, reflection, table, writer);
            try written.insert(table.name);
        }
    }

    // ... then write any remaining tables to preserve schema order if edited by user.
    for (reflection.tables) |table| {
        if (written.contains(table.name)) continue;
        try writeTable(allocator, schema, reflection, table, writer);
    }

    return try self.allocator.dupe(u8, try validateAndFormat(allocator, buf.items));
}

fn writeTable(
    allocator: std.mem.Allocator,
    comptime schema: type,
    reflection: jetquery.Reflection,
    table: jetquery.Reflection.TableInfo,
    writer: anytype,
) !void {
    const model_name = try translateTableName(allocator, schema, table.name);
    try writer.print(
        \\
        \\pub const {s} = jetquery.Table(
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
    , .{try stringifyOptions(allocator, schema, table.name)});
}

fn validateAndFormat(allocator: std.mem.Allocator, input: []const u8) ![]const u8 {
    const ast = try std.zig.Ast.parse(
        allocator,
        try std.mem.concatWithSentinel(allocator, u8, &.{input}, 0),
        .zig,
    );
    if (ast.errors.len > 0) {
        const tty = std.io.tty.detectConfig(std.io.getStdErr());
        const writer = std.io.getStdErr().writer();
        var it = std.mem.tokenizeScalar(u8, input, '\n');
        var line_number: usize = 1;
        while (it.next()) |line| : (line_number += 1) {
            const maybe_err = for (ast.errors) |err| {
                if (ast.tokenLocation(0, err.token).line == line_number + 1) break err;
            } else null;
            try tty.setColor(writer, if (maybe_err != null) .red else .cyan);
            const error_message = if (maybe_err) |err| blk: {
                var buf = std.ArrayList(u8).init(allocator);
                const err_writer = buf.writer();
                try err_writer.writeAll(" // ");
                try ast.renderError(err, err_writer);
                break :blk try buf.toOwnedSlice();
            } else "";
            std.debug.print("{: <4} {s}{s}\n", .{
                line_number,
                line,
                error_message,
            });
        }
        try tty.setColor(writer, .reset);
        std.debug.print("\nFound errors in generated schema.\n", .{});
        return error.JetQueryInvalidSchema;
    }
    return try ast.render(allocator);
}

fn stringifyOptions(
    allocator: std.mem.Allocator,
    comptime schema: type,
    table_name: []const u8,
) ![]const u8 {
    inline for (comptime std.meta.declarations(schema)) |decl| {
        const table = @field(schema, decl.name);
        if (std.mem.eql(u8, table.name, table_name)) {
            return try stringifyModelOptions(allocator, table);
        }
    }

    return ".{}";
}

fn stringifyModelOptions(allocator: std.mem.Allocator, model: type) ![]const u8 {
    var buf = std.ArrayList(u8).init(allocator);
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
                    .belongs_to => "jetquery.relation.belongsTo",
                    .has_many => "jetquery.relation.hasMany",
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

    var buf = std.ArrayList(u8).init(allocator);
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
    var buf = std.ArrayList([]const u8).init(allocator);
    while (it.next()) |token| {
        var dup = try allocator.dupe(u8, token);
        dup[0] = std.ascii.toUpper(dup[0]);
        try buf.append(dup);
    }
    return try util.singularize(allocator, try std.mem.join(allocator, "", buf.items));
}

test "reflect" {
    var admin_repo = try jetquery.Repo.init(
        std.testing.allocator,
        .{
            .adapter = .{
                .postgresql = .{
                    .database = "postgres",
                    .username = "postgres",
                    .hostname = "127.0.0.1",
                    .password = "password",
                    .port = 5432,
                },
            },
        },
    );
    defer admin_repo.deinit();
    try admin_repo.dropDatabase("reflection_test", .{ .if_exists = true });
    try admin_repo.createDatabase("reflection_test", .{});

    var repo = try jetquery.Repo.init(
        std.testing.allocator,
        .{
            .adapter = .{
                .postgresql = .{
                    .database = "reflection_test",
                    .username = "postgres",
                    .hostname = "127.0.0.1",
                    .password = "password",
                    .port = 5432,
                },
            },
        },
    );
    defer repo.deinit();

    try repo.createTable("cats", &.{
        jetquery.schema.table.primaryKey("id", .{}),
        jetquery.schema.table.column("name", .string, .{ .not_null = true }),
        jetquery.schema.table.column("human_id", .integer, .{}),
        jetquery.schema.table.timestamps(.{}),
    }, .{});
    try repo.createTable("humans", &.{
        jetquery.schema.table.primaryKey("id", .{}),
        jetquery.schema.table.column("name", .string, .{ .not_null = true }),
        jetquery.schema.table.timestamps(.{}),
    }, .{});
    try repo.createTable("dogs", &.{
        jetquery.schema.table.primaryKey("id", .{}),
        jetquery.schema.table.column("name", .string, .{ .not_null = true }),
        jetquery.schema.table.column("is_woofy", .boolean, .{}),
        jetquery.schema.table.column("description", .text, .{}),
        jetquery.schema.table.column("bark_rating", .float, .{}),
        jetquery.schema.table.column("food_budget", .decimal, .{}),
        jetquery.schema.table.timestamps(.{}),
    }, .{});

    const Schema = struct {
        pub const Human = jetquery.Table(
            @This(),
            "humans",
            struct { id: i32, name: []const u8 },
            .{
                .relations = .{
                    .cats = jetquery.relation.hasMany(.Cat, .{ .foreign_key = "custom_foreign_key" }),
                },
            },
        );

        pub const Cat = jetquery.Table(
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

    const reflect = init(std.testing.allocator, &repo);
    const schema = try reflect.generateSchema(Schema);
    defer std.testing.allocator.free(schema);
    try std.testing.expectEqualStrings(
        \\const jetquery = @import("jetquery");
        \\
        \\pub const Human = jetquery.Table(
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
        \\            .cats = jetquery.relation.hasMany(.Cat, .{ .foreign_key = "custom_foreign_key" }),
        \\        },
        \\    },
        \\);
        \\
        \\pub const Cat = jetquery.Table(
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
        \\            .human = jetquery.relation.belongsTo(.Human, .{}),
        \\        },
        \\    },
        \\);
        \\
        \\pub const Dog = jetquery.Table(@This(), "dogs", struct {
        \\    id: i32,
        \\    name: []const u8,
        \\    is_woofy: ?bool,
        \\    description: ?[]const u8,
        \\    bark_rating: ?f64,
        \\    food_budget: ?[]const u8,
        \\    created_at: jetquery.DateTime,
        \\    updated_at: jetquery.DateTime,
        \\}, .{});
        \\
    , schema);
}
