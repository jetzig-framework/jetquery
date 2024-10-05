const std = @import("std");

const fields = @import("fields.zig");
const coercion = @import("coercion.zig");

const Where = @This();

fn ClauseValues(context: Node.Context) type {
    return struct {
        values: context.ValuesTuple,
        errors: context.ErrorsTuple,
    };
}

pub fn values(
    comptime context: Node.Context,
    args: anytype,
) ClauseValues(context) {
    var vals: context.ValuesTuple = undefined;
    var errors: context.ErrorsTuple = undefined;
    assignValues(args, context.ValuesTuple, &vals, context.ErrorsTuple, &errors, context.fields, -1);
    return .{ .values = vals, .errors = errors };
}

pub fn tree(Table: type, relations: []const type, T: type, comptime field_context: fields.FieldContext) Node {
    var index: usize = 0;
    return nodeTree(Table, relations, T, T, "root", undefined, &.{}, field_context, &index);
}

pub fn nodeTree(
    Table: type,
    relations: []const type,
    OG: type,
    T: type,
    comptime name: []const u8,
    field_info: std.builtin.Type.StructField,
    comptime path: [][]const u8,
    comptime field_context: fields.FieldContext,
    comptime value_index: *usize,
) Node {
    comptime {
        if (coercion.canCoerceDelegate(T)) {
            const value = Node.Value{
                .field_context = field_context,
                .Table = findRelation(Table, relations, path),
                .name = name,
                .type = T,
                .field_info = field_info,
                .index = value_index.*,
            };
            value_index.* += 1;
            return .{ .value = value };
        }

        return switch (@typeInfo(T)) {
            .@"struct" => |info| blk: {
                var nodes: [info.fields.len]Node = undefined;

                for (info.fields, 0..) |field, index| {
                    var appended_path: [path.len + 1][]const u8 = undefined;
                    for (0..path.len) |idx| appended_path[idx] = path[idx];
                    appended_path[appended_path.len - 1] = field.name;
                    nodes[index] = nodeTree(
                        Table,
                        relations,
                        OG,
                        field.type,
                        field.name,
                        field_info,
                        &appended_path,
                        field_context,
                        value_index,
                    );
                }
                break :blk .{ .group = .{ .name = name, .children = &nodes } };
            },
            .enum_literal => blk: {
                if (path.len == 0) unreachable;

                var t: type = OG;
                for (path[0 .. path.len - 1]) |c| {
                    t = std.meta.FieldType(t, std.enums.nameCast(std.meta.FieldEnum(t), c));
                }
                const value: t = undefined;
                const condition = @field(value, path[path.len - 1]);
                break :blk .{ .condition = condition };
            },
            else => blk: {
                const value = Node.Value{
                    .field_context = field_context,
                    .Table = findRelation(Table, relations, path),
                    .name = name,
                    .type = T,
                    .field_info = field_info,
                    .index = value_index.*,
                };
                value_index.* += 1;
                break :blk .{ .value = value };
            },
        };
    }
}

fn findRelation(Table: type, relations: []const type, comptime path: [][]const u8) type {
    comptime {
        if (path.len <= 1) return Table;
        for (relations) |relation| {
            if (std.mem.eql(u8, relation.relation_name, path[path.len - 2])) return relation.Source;
        }
        return Table;
    }
}

fn assignValues(
    arg: anytype,
    ValuesTuple: type,
    values_tuple: *ValuesTuple,
    ErrorsTuple: type,
    errors_tuple: *ErrorsTuple,
    value_fields: []const Node.Context.Field,
    comptime tuple_index: isize,
) void {
    if (comptime coercion.canCoerceDelegate(@TypeOf(arg))) {
        assignValue(arg, ValuesTuple, values_tuple, ErrorsTuple, errors_tuple, value_fields, tuple_index);
        return;
    }

    switch (@typeInfo(@TypeOf(arg))) {
        .@"struct" => |info| {
            // TODO: There must be a better way of tracking current arg index - we can't pass a
            // comptime pointer to this function without triggering an error (runtime ref to
            // comptime var).
            comptime var idx: isize = tuple_index;
            inline for (info.fields) |field| {
                const T = @TypeOf(@field(arg, field.name));
                idx += if (comptime coercion.canCoerceDelegate(T)) 1 else switch (@typeInfo(T)) {
                    .@"struct", .enum_literal => 0,
                    else => 1,
                };
                assignValues(
                    @field(arg, field.name),
                    ValuesTuple,
                    values_tuple,
                    ErrorsTuple,
                    errors_tuple,
                    value_fields,
                    idx,
                );
            }
        },
        .enum_literal => {},
        else => {
            assignValue(arg, ValuesTuple, values_tuple, ErrorsTuple, errors_tuple, value_fields, tuple_index);
        },
    }
}

fn assignValue(
    arg: anytype,
    ValuesTuple: type,
    values_tuple: *ValuesTuple,
    ErrorsTuple: type,
    errors_tuple: *ErrorsTuple,
    value_fields: []const Node.Context.Field,
    comptime tuple_index: isize,
) void {
    inline for (
        std.meta.fields(ValuesTuple),
        value_fields,
        0..,
    ) |field, value_field, index| {
        const tuple_field_name = std.fmt.comptimePrint("{d}", .{value_field.index});

        if (comptime tuple_index == index) {
            const field_info = comptime fields.fieldInfo(
                field,
                value_field.Table,
                value_field.name,
                value_field.context,
            );
            const coerced: coercion.CoercedValue(value_field.column_type) = coercion.coerce(
                value_field.Table,
                field_info,
                arg,
            );
            @field(values_tuple, tuple_field_name) = coerced.value;
            @field(errors_tuple, tuple_field_name) = coerced.err;
        }
    }
}

pub const Node = union(enum) {
    pub const Condition = enum { NOT, AND, OR };
    pub const Value = struct {
        name: []const u8,
        type: type,
        Table: type,
        field_info: std.builtin.Type.StructField,
        field_context: fields.FieldContext,
        index: usize,

        pub fn ColumnType(self: Value) type {
            return fields.ColumnType(self.Table, fields.fieldInfo(
                self.field_info,
                self.Table,
                self.name,
                self.field_context,
            ));
        }
    };

    pub const Group = struct {
        name: []const u8,
        children: []const Node,
    };

    pub const Context = struct {
        sql: []const u8,
        ValuesTuple: type,
        ErrorsTuple: type,
        len: usize,
        fields: []const Field,

        pub const Field = struct {
            name: []const u8,
            Table: type,
            column_type: type,
            context: fields.FieldContext,
            index: usize,
        };
    };

    const Counter = struct {
        count: usize,

        const Self = @This();

        pub fn write(self: *Self, bytes: []const u8) !void {
            self.count += bytes.len;
        }
        pub fn print(self: *Self, comptime fmt: []const u8, comptime args: anytype) !void {
            self.write(std.fmt.comptimePrint(fmt, args)) catch unreachable;
        }
    };

    condition: Condition,
    value: Value,
    group: Group,

    pub fn render(
        self: Node,
        comptime writer: anytype,
        comptime value_index: *usize,
        comptime depth: usize,
        comptime prev: ?Node,
    ) void {
        switch (self) {
            .condition => |capture| {
                const operator = switch (capture) {
                    .NOT => if (prev == null) "NOT" else "AND NOT",
                    else => |tag| @tagName(tag),
                };
                writer.print(" {s} ", .{operator}) catch unreachable;
            },
            .value => |value| {
                value_index.* += 1;
                // TODO: This needs re-thinking.
                const is_sequence = if (prev) |capture| capture == .value else false;
                const prefix = if (is_sequence) " AND " else "";
                writer.print("{s}{s}.{s} = ${}", .{ prefix, value.Table.name, value.name, value_index.* }) catch unreachable;
            },
            .group => |group| {
                if (group.children.len > 1) writer.print("(", .{}) catch unreachable;
                var prev_child: ?Node = null;
                for (group.children) |child| {
                    if (prev_child) |capture| {
                        if (child == .group and capture == .group) writer.print(" AND ", .{}) catch unreachable;
                    }
                    child.render(writer, value_index, depth + 1, prev_child);
                    prev_child = child;
                }
                if (group.children.len > 1) writer.print(")", .{}) catch unreachable;
            },
        }
    }

    pub fn context(
        comptime self: Node,
        Table: type,
        relations: []const type,
        comptime first_index: usize,
    ) Context {
        comptime {
            var counter = Counter{ .count = 0 };
            var value_index: usize = first_index;
            self.render(&counter, &value_index, 0, null);

            var buf: [counter.count]u8 = undefined;
            var stream = std.io.fixedBufferStream(&buf);
            value_index = 0;
            self.render(stream.writer(), &value_index, 0, null);
            const T = self.ValuesTuple();
            return .{
                .len = std.meta.fields(T).len,
                .fields = &self.value_fields(Table, relations),
                .sql = stream.getWritten() ++ "",
                .ValuesTuple = T,
                .ErrorsTuple = self.ErrorsTuple(),
            };
        }
    }

    fn ValuesTuple(comptime self: Node) type {
        var types: [self.countValues()]type = undefined;
        var index: usize = 0;

        switch (self) {
            .condition => {},
            .value => |value| {
                types[index.*] = value.ColumnType();
                index.* += 1;
            },
            .group => |group| {
                appendValueTypes(group, &types, &index);
            },
        }

        return std.meta.Tuple(&types);
    }

    fn ErrorsTuple(comptime self: Node) type {
        var types: [self.countValues()]type = undefined;
        for (0..self.countValues()) |index| {
            types[index] = ?anyerror;
        }

        return std.meta.Tuple(&types);
    }

    fn value_fields(comptime self: Node, Table: type, relations: []const type) [self.countValues()]Context.Field {
        var fields_array: [self.countValues()]Context.Field = undefined;

        switch (self) {
            .condition => {},
            .value => |value| {
                fields_array[value.index] = .{
                    .Table = value.Table,
                    .name = value.name,
                    .context = value.field_context,
                    .column_type = value.ColumnType(),
                    .index = value.index,
                };
            },
            .group => |group| {
                appendFields(group, Table, relations, &fields_array);
            },
        }

        return fields_array;
    }

    fn countValues(comptime self: Node) usize {
        var count: usize = 0;

        switch (self) {
            .condition => {},
            .value => {
                count += 1;
            },
            .group => |group| {
                countGroupValues(group, &count);
            },
        }

        return count;
    }

    fn countGroupValues(comptime group: Group, comptime count: *usize) void {
        for (group.children) |child| {
            switch (child) {
                .condition => {},
                .value => {
                    count.* += 1;
                },
                .group => |capture| {
                    countGroupValues(capture, count);
                },
            }
        }
    }

    fn appendValueTypes(comptime group: Group, comptime types: []type, comptime index: *usize) void {
        for (group.children) |child| {
            switch (child) {
                .condition => {},
                .value => |value| {
                    types[index.*] = value.ColumnType();
                    index.* += 1;
                },
                .group => |capture| {
                    appendValueTypes(capture, types, index);
                },
            }
        }
    }

    fn appendFields(
        comptime group: Group,
        Table: type,
        relations: []const type,
        comptime fields_array: []Context.Field,
    ) void {
        for (group.children) |child| {
            switch (child) {
                .condition => {},
                .value => |value| {
                    fields_array[value.index] = .{
                        .Table = value.Table,
                        .name = value.name,
                        .context = value.field_context,
                        .column_type = value.ColumnType(),
                        .index = value.index,
                    };
                },
                .group => |capture| {
                    appendFields(capture, Table, relations, fields_array);
                },
            }
        }
    }
};

fn debugNode(comptime node: Node, comptime depth: usize) void {
    const indent = " " ** depth;
    switch (node) {
        .condition => |tag| {
            @compileLog(std.fmt.comptimePrint("{s}{s}", .{ indent, @tagName(tag) }));
        },
        .value => |tag| {
            @compileLog(std.fmt.comptimePrint("{s}{s}", .{ indent, tag.name }));
        },
        .group => |tag| {
            for (tag.children) |child| {
                debugNode(child, depth + 1);
            }
        },
    }
}

fn debug(value: anytype) void {
    @compileLog(std.fmt.comptimePrint("{any}", .{value}));
}
