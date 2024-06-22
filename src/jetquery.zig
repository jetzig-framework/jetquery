const std = @import("std");

pub const Repo = @import("jetquery/Repo.zig");
pub const adapters = @import("jetquery/adapters.zig");

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
    };
}

/// Create a new query by passing a table definition.
/// ```zig
/// const query = Query(Schema.Cats).init(allocator);
/// ```
pub fn Query(T: type) type {
    return struct {
        const Self = @This();

        allocator: std.mem.Allocator,
        where_nodes: []const WhereNode = &.{},
        select_columns: []const Column = &.{},
        limit_bound: ?usize = null,

        /// Initialize a new Query.
        pub fn init(allocator: std.mem.Allocator) Self {
            return .{ .allocator = allocator };
        }

        /// Free resources associated with this query.
        pub fn deinit(self: Self) void {
            self.allocator.free(self.where_nodes);
            self.allocator.free(self.select_columns);
        }

        /// Specify columns to select in the query.
        pub fn select(self: Self, columns: []const std.meta.FieldEnum(T.Definition)) Self {
            return self.merge(.{ .select_columns = columns });
        }

        /// Specify a where clause for the query.
        pub fn where(self: Self, args: anytype) Self {
            inline for (std.meta.fields(@TypeOf(args))) |field| {
                if (!@hasField(T.Definition, field.name)) @compileError("Unknown field: " ++ field.name);
            }

            var nodes: [std.meta.fields(@TypeOf(args)).len]WhereNode = undefined;
            inline for (std.meta.fields(@TypeOf(args)), 0..) |field, index| {
                if (!@hasField(T.Definition, field.name)) @compileError("Unknown field: " ++ field.name);
                const value = switch (@typeInfo(@TypeOf(@field(args, field.name)))) {
                    .Pointer, .Array => .{ .string = @field(args, field.name) },
                    .Int, .ComptimeInt => .{ .integer = @field(args, field.name) },
                    .Float, .ComptimeFloat => .{ .float = @field(args, field.name) },
                    else => @compileError("Unsupported type for field: " ++ field.name),
                };
                nodes[index] = .{ .name = field.name, .value = value };
            }

            return self.merge(.{ .where_nodes = &nodes });
        }

        /// Apply a limit to the query's results.
        pub fn limit(self: Self, bound: usize) Self {
            return self.merge(.{ .limit_bound = bound });
        }

        /// Render the currenty query as SQL.
        pub fn toSql(self: Self, buf: []u8) ![]const u8 {
            var stream = std.io.fixedBufferStream(buf);
            const writer = stream.writer();

            try writer.print("select ", .{});
            for (self.select_columns, 0..) |column, index| {
                try writer.print("{s}{s} ", .{
                    column.name,
                    if (index < self.select_columns.len - 1) "," else "",
                });
            }
            try writer.print("from {s}", .{T.table_name});
            if (self.where_nodes.len > 0) try writer.print(" where ", .{});
            for (self.where_nodes, 0..) |node, index| {
                try writer.print("{s} = ?{s}", .{
                    node.name,
                    if (index < self.select_columns.len - 1) " and " else "",
                });
            }

            if (self.limit_bound) |bound| try writer.print(" limit {}", .{bound});

            return stream.getWritten();
        }

        // Merge the current query with given arguments.
        fn merge(self: Self, args: anytype) Self {
            defer self.deinit();

            var where_nodes = std.ArrayList(WhereNode).init(self.allocator);
            for (if (@hasField(@TypeOf(args), "where_nodes")) args.where_nodes else &.{}) |new_node| {
                for (self.where_nodes) |node| {
                    if (std.mem.eql(u8, node.name, new_node.name)) break;
                } else {
                    where_nodes.append(new_node) catch @panic("OOM");
                }
            }
            if (!@hasField(@TypeOf(args), "where_nodes")) {
                where_nodes.appendSlice(self.where_nodes) catch @panic("OOM");
            }

            var select_columns = std.ArrayList(Column).init(self.allocator);
            for (if (@hasField(@TypeOf(args), "select_columns")) args.select_columns else &.{}) |name| {
                for (self.select_columns) |column| {
                    if (std.mem.eql(u8, column.name, @tagName(name))) break;
                } else {
                    inline for (std.meta.fields(T.Definition)) |field| {
                        if (std.mem.eql(u8, field.name, @tagName(name))) {
                            const column = Column{
                                .name = @tagName(name),
                                .type = switch (@typeInfo(field.type)) {
                                    .Pointer, .Array => .string,
                                    .Int, .ComptimeInt => .integer,
                                    .Float, .ComptimeFloat => .float,
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
                .limit_bound = if (@hasField(@TypeOf(args), "limit_bound")) args.limit_bound else null,
            };
            return cloned;
        }
    };
}

/// A result of an executed query.
pub const Result = struct {
    x: ?usize = null,
};

/// A database column.
pub const Column = struct {
    name: []const u8,
    type: enum { string, integer, float },
};

/// A bound parameter (e.g. used in a where clause).
pub const Value = union(enum) {
    string: []const u8,
    integer: usize,
    float: f64,
};

/// A node in a where clause, e.g. `x = 10`.
const WhereNode = struct {
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
    const sql = try query.toSql(&buf);
    try std.testing.expectEqualStrings("select name, paws from cats", sql);
}

test "where" {
    const Schema = struct {
        pub const Cats = Table("cats", struct { name: []const u8, paws: usize }, .{});
    };

    const paws = std.crypto.random.int(usize);
    const query = Query(Schema.Cats).init(std.testing.allocator)
        .select(&.{ .name, .paws })
        .where(.{ .name = "bar", .paws = paws });
    defer query.deinit();

    var buf: [1024]u8 = undefined;
    const sql = try query.toSql(&buf);
    try std.testing.expectEqualStrings("select name, paws from cats where name = ? and paws = ?", sql);
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
    const sql = try query.toSql(&buf);
    try std.testing.expectEqualStrings("select name, paws from cats limit 100", sql);
}
