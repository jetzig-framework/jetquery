const std = @import("std");

pub const Repo = @import("jetquery/Repo.zig");
pub const adapters = @import("jetquery/adapters.zig");
pub const Migration = @import("jetquery/Migration.zig");
pub const table = @import("jetquery/table.zig");
pub const Row = @import("jetquery/Row.zig");
pub const Result = @import("jetquery/Result.zig").Result;
pub const DateTime = @import("jetquery/DateTime.zig");
pub const events = @import("jetquery/events.zig");

const TableOptions = struct {};

/// Abstraction of a database table. Define a schema with:
/// ```zig
/// const Schema = struct {
///     pub const Cats = Table("cats", struct { name: []const u8, paws: usize }, .{});
/// };
/// ```
pub fn Table(name: []const u8, T: type, options: TableOptions) type {
    _ = options;
    return struct {
        pub const Definition = T;
        pub const table_name = name;

        pub fn insert(repo: Repo, args: anytype) !void {
            try repo.insert(T, args);
        }
    };
}

/// Create a new query by passing a table definition.
/// ```zig
/// const query = Query(Schema.Cats).init(allocator);
/// ```
pub fn Query(T: type) type {
    const QueryType = enum { select, insert, update, delete };

    return struct {
        const Self = @This();
        pub const Definition = T.Definition;

        allocator: std.mem.Allocator,
        where_nodes: []const ParamNode = &.{},
        select_columns: []const Column = &.{},
        insert_nodes: []const ParamNode = &.{},
        update_nodes: []const ParamNode = &.{},
        limit_bound: ?usize = null,
        is_delete: bool = false,

        /// Initialize a new Query.
        pub fn init(allocator: std.mem.Allocator) Self {
            return .{ .allocator = allocator };
        }

        /// Free resources associated with this query.
        pub fn deinit(self: Self) void {
            self.allocator.free(self.where_nodes);
            self.allocator.free(self.select_columns);
            self.allocator.free(self.insert_nodes);
            self.allocator.free(self.update_nodes);
        }

        /// Specify columns to select in the query.
        pub fn select(self: Self, select_columns: []const std.meta.FieldEnum(T.Definition)) Self {
            return self.merge(.{ .select_columns = select_columns });
        }

        /// Specify a where clause for the query.
        pub fn where(self: Self, args: anytype) Self {
            validateFields(args);
            const nodes = buildParamNodes(args);

            return self.merge(.{ .where_nodes = nodes });
        }

        /// Apply a limit to the query's results.
        pub fn limit(self: Self, bound: usize) Self {
            return self.merge(.{ .limit_bound = bound });
        }

        /// Specify values to insert.
        pub fn insert(self: Self, args: anytype) Self {
            validateFields(args);
            const nodes = buildParamNodes(args);

            return self.merge(.{ .insert_nodes = nodes });
        }

        /// Specify values to update.
        pub fn update(self: Self, args: anytype) Self {
            validateFields(args);
            const nodes = buildParamNodes(args);

            return self.merge(.{ .update_nodes = nodes });
        }

        /// Specify delete query.
        pub fn delete(self: Self) Self {
            return self.merge(.{ .is_delete = true });
        }

        /// Render the currenty query as SQL.
        pub fn toSql(self: Self, buf: []u8, adapter: adapters.Adapter) ![]const u8 {
            var stream = std.io.fixedBufferStream(buf);
            const writer = stream.writer();

            try self.validateQueryType();

            if (self.detectQueryType()) |query_type| {
                switch (query_type) {
                    .select => try self.renderSelect(writer, adapter),
                    .insert => try self.renderInsert(writer, adapter),
                    .update => try self.renderUpdate(writer, adapter),
                    .delete => try self.renderDelete(writer, adapter),
                }
            } else return error.JetQueryUndefinedQueryType;

            return stream.getWritten();
        }

        fn validateQueryType(self: Self) !void {
            var count: u3 = 0;

            if (self.select_columns.len > 0) count += 1;
            if (self.insert_nodes.len > 0) count += 1;
            if (self.update_nodes.len > 0) count += 1;
            if (self.is_delete) count += 1;

            if (count != 1) return error.JetQueryIncompatibleQueryType;
        }

        fn detectQueryType(self: Self) ?QueryType {
            if (self.select_columns.len > 0) return .select;
            if (self.insert_nodes.len > 0) return .insert;
            if (self.update_nodes.len > 0) return .update;
            if (self.is_delete) return .delete;

            return null;
        }

        // Merge the current query with given arguments.
        fn merge(self: Self, args: anytype) Self {
            defer self.deinit();

            var where_nodes = std.ArrayList(ParamNode).init(self.allocator);
            for (if (@hasField(@TypeOf(args), "where_nodes")) args.where_nodes else &.{}) |new_node| {
                for (self.where_nodes) |node| {
                    if (std.mem.eql(u8, node.name, new_node.name) and node.value.eql(new_node.value)) continue;
                } else {
                    where_nodes.append(new_node) catch @panic("OOM");
                }
            }
            if (!@hasField(@TypeOf(args), "where_nodes")) {
                where_nodes.appendSlice(self.where_nodes) catch @panic("OOM");
            }

            var insert_nodes = std.ArrayList(ParamNode).init(self.allocator);
            for (if (@hasField(@TypeOf(args), "insert_nodes")) args.insert_nodes else &.{}) |new_node| {
                for (self.insert_nodes) |node| {
                    if (std.mem.eql(u8, node.name, new_node.name) and node.value.eql(new_node.value)) continue;
                } else {
                    insert_nodes.append(new_node) catch @panic("OOM");
                }
            }
            if (!@hasField(@TypeOf(args), "insert_nodes")) {
                insert_nodes.appendSlice(self.insert_nodes) catch @panic("OOM");
            }

            var update_nodes = std.ArrayList(ParamNode).init(self.allocator);
            for (if (@hasField(@TypeOf(args), "update_nodes")) args.update_nodes else &.{}) |new_node| {
                for (self.update_nodes) |node| {
                    if (std.mem.eql(u8, node.name, new_node.name) and node.value.eql(new_node.value)) continue;
                } else {
                    update_nodes.append(new_node) catch @panic("OOM");
                }
            }
            if (!@hasField(@TypeOf(args), "update_nodes")) {
                update_nodes.appendSlice(self.update_nodes) catch @panic("OOM");
            }

            var select_columns = std.ArrayList(Column).init(self.allocator);
            for (if (@hasField(@TypeOf(args), "select_columns")) args.select_columns else &.{}) |name| {
                for (self.select_columns) |column| {
                    if (std.mem.eql(u8, column.name, @tagName(name))) continue;
                } else {
                    inline for (std.meta.fields(T.Definition)) |field| {
                        if (std.mem.eql(u8, field.name, @tagName(name))) {
                            const column = if (field.type == DateTime)
                                Column{ .name = @tagName(name), .type = .datetime }
                            else
                                Column{
                                    .name = @tagName(name),
                                    .type = switch (@typeInfo(field.type)) {
                                        .pointer, .array => .string,
                                        .int, .comptime_int => .integer,
                                        .float, .comptime_float => .float,
                                        else => @compileError("Unsupported type " ++ @typeName(field.type)),
                                    },
                                };
                            select_columns.append(column) catch @panic("OOM");
                            break;
                        }
                    }
                }
            }
            if (!@hasField(@TypeOf(args), "select_columns")) {
                select_columns.appendSlice(self.select_columns) catch @panic("OOM");
            }

            const cloned: Self = .{
                .allocator = self.allocator,
                .where_nodes = where_nodes.toOwnedSlice() catch @panic("OOM"),
                .select_columns = select_columns.toOwnedSlice() catch @panic("OOM"),
                .insert_nodes = insert_nodes.toOwnedSlice() catch @panic("OOM"),
                .update_nodes = update_nodes.toOwnedSlice() catch @panic("OOM"),
                .limit_bound = if (@hasField(@TypeOf(args), "limit_bound")) args.limit_bound else null,
                .is_delete = self.is_delete or (@hasField(@TypeOf(args), "is_delete") and args.is_delete),
            };
            return cloned;
        }

        fn validateFields(args: anytype) void {
            inline for (std.meta.fields(@TypeOf(args))) |field| {
                if (!@hasField(T.Definition, field.name)) @compileError("Unknown field: " ++ field.name);
            }
        }

        fn buildParamNodes(args: anytype) []const ParamNode {
            var nodes: [std.meta.fields(@TypeOf(args)).len]ParamNode = undefined;
            inline for (std.meta.fields(@TypeOf(args)), 0..) |field, index| {
                if (!@hasField(T.Definition, field.name)) @compileError("Unknown field: " ++ field.name);
                const value = switch (@typeInfo(@TypeOf(@field(args, field.name)))) {
                    .pointer, .array => .{ .string = @field(args, field.name) },
                    .int, .comptime_int => .{ .integer = @field(args, field.name) },
                    .float, .comptime_float => .{ .float = @field(args, field.name) },
                    else => @compileError("Unsupported type for field: " ++ field.name),
                };
                nodes[index] = .{ .name = field.name, .value = value };
            }
            return &nodes;
        }

        fn renderSelect(self: Self, writer: anytype, adapter: adapters.Adapter) !void {
            try writer.print("select ", .{});
            for (self.select_columns, 0..) |column, index| {
                try writer.print("{}{s} ", .{
                    adapter.identifier(column.name),
                    if (index < self.select_columns.len - 1) "," else "",
                });
            }
            try writer.print("from {}", .{adapter.identifier(T.table_name)});
            try self.renderWhere(writer, adapter);
            if (self.limit_bound) |bound| try writer.print(" limit {}", .{bound});
        }

        fn renderInsert(self: Self, writer: anytype, adapter: adapters.Adapter) !void {
            try writer.print("insert into {} (", .{adapter.identifier(T.table_name)});
            for (self.insert_nodes, 0..) |node, index| {
                try writer.print("{}{s}", .{
                    adapter.identifier(node.name),
                    if (index < self.insert_nodes.len - 1) ", " else "",
                });
            }
            try writer.print(") values (", .{});
            for (self.insert_nodes, 0..) |node, index| {
                var buf: [1024]u8 = undefined;
                try writer.print("{s}{s}", .{
                    try node.value.toSql(&buf),
                    if (index + 1 < self.insert_nodes.len) ", " else ")",
                });
            }
        }

        fn renderUpdate(self: Self, writer: anytype, adapter: adapters.Adapter) !void {
            try writer.print("update {} set ", .{adapter.identifier(T.table_name)});
            for (self.update_nodes, 0..) |node, index| {
                var buf: [1024]u8 = undefined;
                try writer.print("{} = {s}{s}", .{
                    adapter.identifier(node.name),
                    try node.value.toSql(&buf),
                    if (index < self.update_nodes.len - 1) ", " else "",
                });
            }
            try self.renderWhere(writer, adapter);
        }

        fn renderDelete(self: Self, writer: anytype, adapter: adapters.Adapter) !void {
            try writer.print("delete from {}", .{adapter.identifier(T.table_name)});
            try self.renderWhere(writer, adapter);
        }

        fn renderWhere(self: Self, writer: anytype, adapter: adapters.Adapter) !void {
            if (self.where_nodes.len == 0) return;

            try writer.print(" where ", .{});
            for (self.where_nodes, 0..) |node, index| {
                var buf: [1024]u8 = undefined;
                try writer.print(
                    \\{} = {s}{s}
                , .{
                    adapter.identifier(node.name),
                    try node.value.toSql(&buf),
                    if (index + 1 < self.select_columns.len) " and " else "",
                });
            }
        }
    };
}

/// A database column.
pub const Column = struct {
    name: []const u8,
    type: Type,
    options: Options = .{},
    primary_key: bool = false,
    timestamps: bool = false,

    pub const Type = enum { string, integer, float, decimal, datetime, text };
    pub const Options = struct {};

    pub fn init(name: []const u8, column_type: Type, options: Options) Column {
        return .{ .name = name, .type = column_type, .options = options };
    }
};

/// A bound parameter (e.g. used in a where clause).
pub const Value = union(enum) {
    string: []const u8,
    integer: usize,
    float: f64,
    boolean: bool,
    Null: void,

    pub fn toSql(self: Value, buf: []u8) ![]const u8 {
        // TODO: Prevent SQL injection
        var stream = std.io.fixedBufferStream(buf);
        const writer = stream.writer();
        switch (self) {
            .string => |value| try writer.print("'{s}'", .{value}),
            .integer => |value| try writer.print("{}", .{value}),
            .float => |value| try writer.print("{}", .{value}),
            .boolean => |value| try writer.print("{}", .{@as(u1, if (value) 1 else 0)}),
            .Null => try writer.print("NULL", .{}),
        }
        return stream.getWritten();
    }

    pub fn eql(self: Value, other: Value) bool {
        return switch (self) {
            .string => |value| other == .string and std.mem.eql(u8, value, other.string),
            .integer => |value| other == .integer and value == other.integer,
            .float => |value| other == .float and value == other.float,
            .boolean => |value| other == .boolean and value == other.boolean,
            .Null => other == .Null,
        };
    }
};

pub const Identifier = struct {
    name: []const u8,
    quote_char: u8,

    pub fn format(self: Identifier, actual_fmt: []const u8, options: anytype, writer: anytype) !void {
        // TODO: SQL injection
        _ = actual_fmt;
        _ = options;
        try writer.print("{0c}{1s}{0c}", .{ self.quote_char, self.name });
    }
};

/// A node in a where clause, e.g. `x = 10`.
const ParamNode = struct {
    name: []const u8,
    value: Value,
};

test {
    std.testing.refAllDeclsRecursive(@This());
}

test "select" {
    const Schema = struct {
        pub const Cats = Table("cats", struct { name: []const u8, paws: usize }, .{});
    };
    const query = Query(Schema.Cats).init(std.testing.allocator)
        .select(&.{ .name, .paws });
    defer query.deinit();

    var buf: [1024]u8 = undefined;
    const sql = try query.toSql(&buf, adapters.test_adapter);
    try std.testing.expectEqualStrings(
        \\select "name", "paws" from "cats"
    , sql);
}

test "where" {
    const Schema = struct {
        pub const Cats = Table("cats", struct { name: []const u8, paws: usize }, .{});
    };

    const paws = 4;
    const query = Query(Schema.Cats).init(std.testing.allocator)
        .select(&.{ .name, .paws })
        .where(.{ .name = "bar", .paws = paws });
    defer query.deinit();

    var buf: [1024]u8 = undefined;
    const sql = try query.toSql(&buf, adapters.test_adapter);
    try std.testing.expectEqualStrings(
        \\select "name", "paws" from "cats" where "name" = 'bar' and "paws" = 4
    , sql);
    try std.testing.expectEqualStrings(query.where_nodes[0].name, "name");
    try std.testing.expectEqualStrings(query.where_nodes[0].value.string, "bar");
    try std.testing.expectEqualStrings(query.where_nodes[1].name, "paws");
    try std.testing.expect(query.where_nodes[1].value == .integer);
}

test "limit" {
    const Schema = struct {
        pub const Cats = Table("cats", struct { name: []const u8, paws: usize }, .{});
    };
    const query = Query(Schema.Cats).init(std.testing.allocator)
        .select(&.{ .name, .paws })
        .limit(100);
    defer query.deinit();

    var buf: [1024]u8 = undefined;
    const sql = try query.toSql(&buf, adapters.test_adapter);
    try std.testing.expectEqualStrings(
        \\select "name", "paws" from "cats" limit 100
    , sql);
}

test "insert" {
    const Schema = struct {
        pub const Cats = Table("cats", struct { name: []const u8, paws: usize }, .{});
    };
    const query = Query(Schema.Cats).init(std.testing.allocator)
        .insert(.{ .name = "Hercules", .paws = 4 });
    defer query.deinit();

    var buf: [1024]u8 = undefined;
    try std.testing.expectEqualStrings(
        \\insert into "cats" ("name", "paws") values ('Hercules', 4)
    ,
        try query.toSql(&buf, adapters.test_adapter),
    );
}

test "update" {
    const Schema = struct {
        pub const Cats = Table("cats", struct { name: []const u8, paws: usize }, .{});
    };
    const query = Query(Schema.Cats).init(std.testing.allocator)
        .update(.{ .name = "Heracles", .paws = 2 })
        .where(.{ .name = "Hercules" });

    defer query.deinit();
    var buf: [1024]u8 = undefined;
    try std.testing.expectEqualStrings(
        \\update "cats" set "name" = 'Heracles', "paws" = 2 where "name" = 'Hercules'
    ,
        try query.toSql(&buf, adapters.test_adapter),
    );
}

test "delete" {
    const Schema = struct {
        pub const Cats = Table("cats", struct { name: []const u8, paws: usize }, .{});
    };
    const query = Query(Schema.Cats).init(std.testing.allocator)
        .delete()
        .where(.{ .name = "Hercules" });
    defer query.deinit();

    var buf: [1024]u8 = undefined;
    try std.testing.expectEqualStrings(
        \\delete from "cats" where "name" = 'Hercules'
    ,
        try query.toSql(&buf, adapters.test_adapter),
    );
}
