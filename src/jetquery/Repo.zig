const std = @import("std");

const jetquery = @import("../jetquery.zig");

allocator: std.mem.Allocator,
adapter: jetquery.adapters.Adapter,
result_map: std.AutoHashMap(i128, jetquery.Result),
result_id: std.atomic.Value(i128) = std.atomic.Value(i128).init(0),
eventCallback: *const fn (event: jetquery.events.Event) anyerror!void = jetquery.events.defaultCallback,

const Repo = @This();

const InitOptions = struct {
    adapter: union(enum) {
        postgresql: jetquery.adapters.PostgresqlAdapter.Options,
        null,
    },
    eventCallback: *const fn (event: jetquery.events.Event) anyerror!void = jetquery.events.defaultCallback,
    lazy_connect: bool = false,
};

/// Initialize a new Repo for executing queries.
pub fn init(allocator: std.mem.Allocator, options: InitOptions) !Repo {
    return .{
        .allocator = allocator,
        .result_map = std.AutoHashMap(i128, jetquery.Result).init(allocator),
        .adapter = switch (options.adapter) {
            .postgresql => |adapter_options| .{
                .postgresql = try jetquery.adapters.PostgresqlAdapter.init(
                    allocator,
                    adapter_options,
                    options.lazy_connect,
                ),
            },
            .null => .{ .null = jetquery.adapters.NullAdapter{} },
        },
        .eventCallback = options.eventCallback,
    };
}

const GlobalOptions = struct {
    eventCallback: *const fn (event: jetquery.events.Event) anyerror!void = jetquery.events.defaultCallback,
    lazy_connect: bool = false,
};

/// Initialize a new repo using a config file. Config file build path is configured by build
/// option `jetquery_config_path`.
pub fn loadConfig(allocator: std.mem.Allocator, global_options: GlobalOptions) !Repo {
    const AdapterOptions = switch (jetquery.adapter) {
        .postgresql => jetquery.adapters.PostgresqlAdapter.Options,
        .null => jetquery.adapters.NullAdapter.Options,
    };
    var options: AdapterOptions = undefined;
    inline for (std.meta.fields(AdapterOptions)) |field| {
        if (@hasField(@TypeOf(jetquery.config.database), field.name)) {
            @field(options, field.name) = @field(jetquery.config.database, field.name);
        } else if (field.default_value) |default| {
            const option: *field.type = @ptrCast(@alignCast(@constCast(default)));
            @field(options, field.name) = option.*;
        } else {
            @compileError("Missing database configuration value for: `" ++ field.name ++ "`");
        }
    }
    var init_options: InitOptions = switch (jetquery.adapter) {
        .postgresql => .{ .adapter = .{ .postgresql = options } },
        .null => .{ .adapter = .null },
    };

    inline for (std.meta.fields(GlobalOptions)) |field| {
        @field(init_options, field.name) = @field(global_options, field.name);
    }
    return Repo.init(allocator, init_options);
}

/// Close connections and free resources.
pub fn deinit(self: *Repo) void {
    switch (self.adapter) {
        inline else => |*adapter| adapter.deinit(),
    }
    self.result_map.deinit();
}

/// Execute the given query and return results.
pub fn execute(self: *Repo, query: anytype) !switch (@TypeOf(query).ResultContext) {
    .one => ?@TypeOf(query).ResultType,
    .many => jetquery.Result,
    .none => void,
} {
    try query.validateValues();
    try query.validateDelete();
    var result = try self.adapter.execute(self, query.sql, query.field_values);
    return switch (@TypeOf(query).ResultContext) {
        .one => blk: {
            if (query.query_context == .count) {
                defer result.deinit();
                return try result.unary(@TypeOf(query).ResultType);
            }
            const row = try result.next(query);
            if (row) |capture| try self.result_map.put(capture.__jetquery_id, result);
            break :blk row;
        },
        .many => result,
        .none => blk: {
            defer result.deinit();
            break :blk {};
        },
    };
}

pub fn save(self: *Repo, value: anytype) !void {
    // TODO: Infer primary key instead of assuming `id` - we can set this in the schema as an
    // option to each table - we already have the table available as `value.__jetquery_model`.

    // XXX: We have to include all (selected) values in the UPDATE because we can't generate a
    // type for the `update` params (which becomes a tuple passed to pg.zig) at runtime - ideally
    // we would only include modified values but we would need to generate all possible
    // combinations of updates in order to do this which is not practical. I don't know if there
    // is a way around this.
    if (!self.isModified(value)) return;

    comptime var size: usize = 0;
    comptime {
        for (std.meta.fields(@TypeOf(value))) |field| {
            if (!std.mem.startsWith(u8, field.name, "__") and !std.mem.eql(u8, field.name, "id")) {
                size += 1;
            }
        }
    }
    comptime var fields: [size]std.builtin.Type.StructField = undefined;
    comptime {
        var index: usize = 0;
        for (std.meta.fields(@TypeOf(value))) |field| {
            if (!std.mem.startsWith(u8, field.name, "__") and !std.mem.eql(u8, field.name, "id")) {
                fields[index] = field;
                index += 1;
            }
        }
    }

    const Update = @Type(.{
        .@"struct" = .{
            .layout = .auto,
            .fields = &fields,
            .decls = &.{},
            .is_tuple = false,
        },
    });

    var update: Update = undefined;

    inline for (std.meta.fields(Update)) |field| {
        @field(update, field.name) = @field(value, field.name);
    }

    const query = jetquery.Query(value.__jetquery_schema, value.__jetquery_model_name)
        .update(update)
        .where(.{ .id = value.id });

    try self.execute(query);
}

pub const FieldState = struct {
    name: []const u8,
    modified: bool,
};

pub fn isModified(self: Repo, value: anytype) bool {
    const field_states = self.fieldStates(value);
    for (field_states) |field_state| {
        if (field_state.modified) return true;
    }
    return false;
}

pub fn fieldStates(self: Repo, value: anytype) []const FieldState {
    _ = self;
    comptime var size: usize = 0;
    inline for (std.meta.fields(@TypeOf(value))) |field| {
        if (comptime std.mem.startsWith(u8, field.name, "__")) size += 1;
    }
    var field_state: [size]FieldState = undefined;
    var index: usize = 0;

    inline for (std.meta.fields(@TypeOf(value))) |field| {
        if (!comptime std.mem.startsWith(u8, field.name, jetquery.original_prefix)) continue;

        const modified = switch (@typeInfo(field.type)) {
            .pointer => |info| std.mem.eql(
                info.child,
                @field(value, field.name),
                @field(value, field.name[jetquery.original_prefix.len..]),
            ),
            else => @field(value, field.name) == @field(value, field.name[jetquery.original_prefix.len..]),
        };

        field_state[index] = .{ .name = field.name, .modified = modified };
        index += 1;
    }
    return &field_state;
}

/// Free a single result's allocated memory. Use in conjunction with `findBy` and `find` as these
/// methods simply return structs as defined by the Schema and do not have another way to deinit.
pub fn free(self: *Repo, value: anytype) void {
    if (self.result_map.getEntry(value.__jetquery_id)) |entry| {
        entry.value_ptr.deinit();
        _ = self.result_map.remove(value.__jetquery_id);
    }
}

pub fn generateId(self: *Repo) i128 {
    // We can probably risk wrapping here if an app loads >i128 records without freeing them
    return self.result_id.fetchAdd(1, .monotonic);
}

pub const CreateTableOptions = struct { if_not_exists: bool = false };

/// Create a database table named `nme`. Pass `.{ .if_not_exists = true }` to use
/// `CREATE TABLE IF NOT EXISTS` syntax.
pub fn createTable(
    self: *Repo,
    comptime name: []const u8,
    comptime columns: []const jetquery.Column,
    options: CreateTableOptions,
) !void {
    var buf = std.ArrayList(u8).init(self.allocator);
    defer buf.deinit();

    const writer = buf.writer();

    try writer.print(
        \\CREATE TABLE{s} {s} (
    , .{ if (options.if_not_exists) " IF NOT EXISTS" else "", self.adapter.identifier(name) });

    inline for (columns, 0..) |column, index| {
        if (column.timestamps) {
            try writer.print(
                \\{s}{s}{s}, {s}{s}{s}{s}
            , .{
                self.adapter.identifier(jetquery.default_column_names.created_at),
                self.adapter.columnTypeSql(.datetime),
                self.adapter.notNullSql(),
                self.adapter.identifier(jetquery.default_column_names.updated_at),
                self.adapter.columnTypeSql(.datetime),
                self.adapter.notNullSql(),
                if (index < columns.len - 1) ", " else "",
            });
        } else {
            try writer.print(
                \\{s}{s}{s}{s}{s}
            , .{
                self.adapter.identifier(column.name),
                if (column.primary_key) "" else self.adapter.columnTypeSql(column.type),
                if (!column.primary_key and column.options.not_null) self.adapter.notNullSql() else "",
                if (column.primary_key) self.adapter.primaryKeySql() else "",
                if (index < columns.len - 1) ", " else "",
            });
        }
    }

    try writer.print(")", .{});
    var result = try self.adapter.execute(self, buf.items, &.{});
    try result.drain();
    defer result.deinit();
}

pub const DropTableOptions = struct { if_exists: bool = false };

/// Drop a database table named `name`. Pass `.{ .if_exists = true }` to use
/// `DROP TABLE IF EXISTS` syntax.
pub fn dropTable(self: *Repo, comptime name: []const u8, options: DropTableOptions) !void {
    var buf = std.ArrayList(u8).init(self.allocator);
    defer buf.deinit();

    const writer = buf.writer();

    try writer.print(
        \\DROP TABLE{s} "{s}"
    , .{ if (options.if_exists) " IF EXISTS" else "", name });

    var result = try self.adapter.execute(self, buf.items, &.{});
    try result.drain();
    defer result.deinit();
}

test "Repo" {
    var repo = try Repo.init(
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
    defer repo.deinit();

    const Schema = struct {
        pub const Cat = jetquery.Table("cats", struct { name: []const u8, paws: i32 }, .{});
    };

    var drop_table = try repo.adapter.execute(&repo, "drop table if exists cats", &.{});
    defer drop_table.deinit();

    var create_table = try repo.adapter.execute(&repo, "create table cats (name varchar(255), paws int)", &.{});
    defer create_table.deinit();

    try jetquery.Query(Schema, .Cat).insert(.{ .name = "Hercules", .paws = 4 }).execute(&repo);
    try jetquery.Query(Schema, .Cat).insert(.{ .name = "Princes", .paws = 4 }).execute(&repo);

    const query = jetquery.Query(Schema, .Cat)
        .select(.{ .name, .paws })
        .where(.{ .paws = 4 });

    var result = try repo.execute(query);
    defer result.deinit();

    while (try result.next(query)) |row| {
        try std.testing.expectEqualStrings("Hercules", row.name);
        try std.testing.expectEqual(4, row.paws);
        break;
    } else {
        try std.testing.expect(false);
    }

    const count_all = try query.count().execute(&repo);
    try std.testing.expectEqual(2, count_all);

    const count_distinct = try jetquery.Query(Schema, .Cat).distinct(.{.paws}).count().execute(&repo);
    try std.testing.expectEqual(1, count_distinct);
}

test "Repo.loadConfig" {
    // Loads default config file: `jetquery.config.zig`
    var repo = try Repo.loadConfig(std.testing.allocator, .{});
    defer repo.deinit();
    try std.testing.expect(repo.adapter == .postgresql);
}

test "relations" {
    var repo = try Repo.init(
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
    defer repo.deinit();

    const Schema = struct {
        pub const Cat = jetquery.Table(
            "cats",
            struct { id: i32, owner_id: i32, name: []const u8, paws: i32 },
            .{ .relations = .{ .owner = jetquery.relation.belongsTo(.Owner, .{}) } },
        );

        pub const Owner = jetquery.Table(
            "owners",
            struct { id: i32, name: []const u8 },
            .{ .relations = .{ .cats = jetquery.relation.hasMany(.Cat, .{}) } },
        );
    };

    try repo.dropTable("cats", .{ .if_exists = true });
    try repo.dropTable("owners", .{ .if_exists = true });

    try repo.createTable(
        "cats",
        &.{
            jetquery.table.column("id", .integer, .{}),
            jetquery.table.column("owner_id", .integer, .{}),
            jetquery.table.column("name", .string, .{}),
            jetquery.table.column("paws", .integer, .{}),
        },
        .{},
    );

    try repo.createTable(
        "owners",
        &.{
            jetquery.table.column("id", .integer, .{}),
            jetquery.table.column("name", .string, .{}),
        },
        .{},
    );

    try jetquery.Query(Schema, .Cat)
        .insert(.{ .id = 1, .name = "Hercules", .paws = 4, .owner_id = 1 })
        .execute(&repo);
    try jetquery.Query(Schema, .Owner)
        .insert(.{ .id = 1, .name = "Bob" })
        .execute(&repo);

    const query = jetquery.Query(Schema, .Cat)
        .include(.owner, .{})
        .findBy(.{ .name = "Hercules" });

    if (try query.execute(&repo)) |cat| {
        defer repo.free(cat);
        try std.testing.expectEqualStrings("Hercules", cat.name);
        try std.testing.expectEqual(4, cat.paws);
        try std.testing.expectEqualStrings("Bob", cat.owner.name);
    } else {
        try std.testing.expect(false);
    }
}

test "timestamps" {
    var repo = try Repo.init(
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
    defer repo.deinit();

    const Schema = struct {
        pub const Cat = jetquery.Table(
            "cats",
            struct {
                id: i32,
                name: []const u8,
                paws: i32,
                created_at: jetquery.DateTime,
                updated_at: jetquery.DateTime,
            },
            .{},
        );
    };

    try repo.dropTable("cats", .{ .if_exists = true });

    try repo.createTable(
        "cats",
        &.{
            jetquery.table.primaryKey("id", .{}),
            jetquery.table.column("name", .string, .{}),
            jetquery.table.column("paws", .integer, .{}),
            jetquery.table.column("created_at", .datetime, .{}),
            jetquery.table.column("updated_at", .datetime, .{}),
        },
        .{},
    );

    const now = std.time.microTimestamp();
    std.time.sleep(std.time.ns_per_ms);

    try jetquery.Query(Schema, .Cat).insert(.{ .name = "Hercules", .paws = 4 }).execute(&repo);
    const maybe_cat = try jetquery.Query(Schema, .Cat).findBy(.{ .name = "Hercules" }).execute(&repo);
    if (maybe_cat) |cat| {
        defer repo.free(cat);
        try std.testing.expect(cat.created_at.microseconds() > now);
    }
}

test "save" {
    var repo = try Repo.init(
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
    defer repo.deinit();

    const Schema = struct {
        pub const Cat = jetquery.Table("cats", struct { id: i32, name: []const u8, paws: i32 }, .{});
    };

    var drop_table = try repo.adapter.execute(&repo, "drop table if exists cats", &.{});
    defer drop_table.deinit();

    var create_table = try repo.adapter.execute(
        &repo,
        "create table cats (id int, name varchar(255), paws int)",
        &.{},
    );
    defer create_table.deinit();

    try jetquery.Query(Schema, .Cat).insert(.{ .id = 1000, .name = "Hercules", .paws = 4 }).execute(&repo);

    var cat = try jetquery.Query(Schema, .Cat)
        .findBy(.{ .name = "Hercules" })
        .execute(&repo) orelse return std.testing.expect(false);
    defer repo.free(cat);

    cat.name = "Princes";
    try repo.save(cat);

    const updated_cat = try jetquery.Query(Schema, .Cat)
        .find(1000)
        .execute(&repo) orelse return std.testing.expect(false);
    defer repo.free(updated_cat);
    try std.testing.expectEqualStrings("Princes", updated_cat.name);
}
