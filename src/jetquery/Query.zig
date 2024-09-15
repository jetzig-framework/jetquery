const std = @import("std");

const jetquery = @import("../jetquery.zig");

/// A node in a where clause, e.g. `x = 10`.
const ParamNode = struct {
    name: []const u8,
    value: jetquery.Value,
};

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
        select_columns: []const jetquery.Column = &.{},
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
            const nodes = self.buildParamNodes(args);

            return self.merge(.{ .where_nodes = nodes });
        }

        /// Apply a limit to the query's results.
        pub fn limit(self: Self, bound: usize) Self {
            return self.merge(.{ .limit_bound = bound });
        }

        /// Specify values to insert.
        pub fn insert(self: Self, args: anytype) Self {
            validateFields(args);
            const nodes = self.buildParamNodes(args);

            return self.merge(.{ .insert_nodes = nodes });
        }

        /// Specify values to update.
        pub fn update(self: Self, args: anytype) Self {
            validateFields(args);
            const nodes = self.buildParamNodes(args);

            return self.merge(.{ .update_nodes = nodes });
        }

        /// Specify delete query.
        pub fn delete(self: Self) Self {
            return self.merge(.{ .is_delete = true });
        }

        /// Render the currenty query as SQL.
        pub fn toSql(self: Self, buf: []u8, adapter: jetquery.adapters.Adapter) ![]const u8 {
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

            var select_columns = std.ArrayList(jetquery.Column).init(self.allocator);
            for (if (@hasField(@TypeOf(args), "select_columns")) args.select_columns else &.{}) |name| {
                for (self.select_columns) |column| {
                    if (std.mem.eql(u8, column.name, @tagName(name))) continue;
                } else {
                    inline for (std.meta.fields(T.Definition)) |field| {
                        if (std.mem.eql(u8, field.name, @tagName(name))) {
                            const column = if (field.type == jetquery.DateTime)
                                jetquery.Column{ .name = @tagName(name), .type = .datetime }
                            else
                                jetquery.Column{
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

        fn buildParamNodes(self: Self, args: anytype) []const ParamNode {
            var nodes: [std.meta.fields(@TypeOf(args)).len]ParamNode = undefined;
            inline for (std.meta.fields(@TypeOf(args)), 0..) |field, index| {
                if (!@hasField(T.Definition, field.name)) @compileError("Unknown field: " ++ field.name);
                const value = switch (@typeInfo(@TypeOf(@field(args, field.name)))) {
                    .pointer => |info| switch (info.size) {
                        .Slice => .{ .string = @field(args, field.name) },
                        else => blk: {
                            const child_info = @typeInfo(info.child);
                            if (child_info == .array) {
                                const arr = &child_info.array;
                                if (arr.child == u8) break :blk .{ .string = @field(args, field.name) };
                            }
                            if (@hasDecl(info.child, "toJetquery")) break :blk self.coerceValue(args, field.name);
                            @compileError("Unsupported type for field: " ++ field.name ++ "(" ++ @typeName(info.child) ++ ")");
                        },
                    },
                    .array => .{ .string = @field(args, field.name) },
                    .int, .comptime_int => .{ .integer = @field(args, field.name) },
                    .float, .comptime_float => .{ .float = @field(args, field.name) },
                    else => if (@hasDecl(@TypeOf(@field(args, field.name)), "toJetquery")) self.coerceValue(args, field.name) else @compileError(
                        "Unsupported type for field: " ++ field.name ++ "(" ++ @typeName(field.type) ++ ")",
                    ),
                };
                nodes[index] = .{ .name = field.name, .value = value };
            }
            return &nodes;
        }

        fn FieldType(C: type, comptime name: []const u8) type {
            inline for (std.meta.fields(C)) |field| {
                if (std.mem.eql(u8, field.name, name)) return field.type;
            }
            @compileError("Type `" ++ @typeName(T) ++ "` does not define field `" ++ name ++ "`");
        }

        fn coerceValue(self: Self, args: anytype, comptime field_name: []const u8) jetquery.Value {
            return switch (FieldType(T.Definition, field_name)) {
                []const u8 => .{
                    .string = @field(args, field_name).toJetquery([]const u8, self.allocator),
                },
                usize => .{
                    .integer = @field(args, field_name).toJetquery(usize, self.allocator),
                },
                f64 => .{
                    .float = @field(args, field_name).toJetquery(f64, self.allocator),
                },
                bool => .{
                    .boolean = @field(args, field_name).toJetquery(bool, self.allocator),
                },
                else => |C| @compileError("Unsupported schema field type `" ++ @typeName(C) ++ "` for field `" ++ field_name ++ "`"),
            };
        }

        fn renderSelect(self: Self, writer: anytype, adapter: jetquery.adapters.Adapter) !void {
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

        fn renderInsert(self: Self, writer: anytype, adapter: jetquery.adapters.Adapter) !void {
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

        fn renderUpdate(self: Self, writer: anytype, adapter: jetquery.adapters.Adapter) !void {
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

        fn renderDelete(self: Self, writer: anytype, adapter: jetquery.adapters.Adapter) !void {
            try writer.print("delete from {}", .{adapter.identifier(T.table_name)});
            try self.renderWhere(writer, adapter);
        }

        fn renderWhere(self: Self, writer: anytype, adapter: jetquery.adapters.Adapter) !void {
            if (self.where_nodes.len == 0) return;

            try writer.print(" where ", .{});
            for (self.where_nodes, 0..) |node, index| {
                var buf: [1024]u8 = undefined;
                try writer.print(
                    \\{} = {s}{s}
                , .{
                    adapter.identifier(node.name),
                    try node.value.toSql(&buf),
                    if (index + 1 < self.where_nodes.len) " and " else "",
                });
            }
        }
    };
}
