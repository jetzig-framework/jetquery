const std = @import("std");

const fields = @import("../fields.zig");
const coercion = @import("../coercion.zig");

const Where = @This();

fn ClauseValues(ValuesTuple: type, ErrorsTuple: type) type {
    return struct {
        values: ValuesTuple,
        errors: ErrorsTuple,
    };
}

pub const Field = struct {
    name: []const u8,
    Table: type,
    column_type: type,
    context: fields.FieldContext,
    index: usize,
};

pub const Tree = struct {
    Table: type,
    relations: []const type,
    root: Node,
    values_count: usize,
    values_fields: []const Field,
    ValuesTuple: type,
    ErrorsTuple: type,

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

    pub fn render(comptime self: Tree, Adapter: type) []const u8 {
        comptime {
            var counter = Counter{ .count = 0 };
            self.root.render(Adapter, &counter, 0, null);

            var buf: [counter.count]u8 = undefined;
            var stream = std.io.fixedBufferStream(&buf);
            self.root.render(Adapter, stream.writer(), 0, null);

            return stream.getWritten() ++ "";
        }
    }

    pub fn values(comptime self: Tree, args: anytype) ClauseValues(self.ValuesTuple, self.ErrorsTuple) {
        var vals: self.ValuesTuple = undefined;
        var errors: self.ErrorsTuple = undefined;
        assignValues(args, self.ValuesTuple, &vals, self.ErrorsTuple, &errors, self.values_fields, 0);
        return .{ .values = vals, .errors = errors };
    }

    pub fn fields(comptime self: Tree) [self.root.countValues()]Field {
        return self.root.values_fields(self.Table, self.relations);
    }

    pub fn countValues(comptime self: Tree) usize {
        return self.root.countValues();
    }
};

pub fn tree(
    Table: type,
    relations: []const type,
    T: type,
    comptime field_context: fields.FieldContext,
    comptime first_value_index: usize,
) Tree {
    var index: usize = first_value_index;
    const root = nodeTree(Table, relations, T, T, "root", undefined, &.{}, field_context, &index);

    return .{
        .root = root,
        .Table = Table,
        .relations = relations,
        .values_count = root.countValues(),
        .values_fields = &root.values_fields(Table, relations),
        .ValuesTuple = root.ValuesTuple(),
        .ErrorsTuple = root.ErrorsTuple(),
    };
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
            const T = fields.ColumnType(self.Table, fields.fieldInfo(
                self.field_info,
                self.Table,
                self.name,
                self.field_context,
            ));
            return if (self.isArray()) []const T else T;
        }

        pub fn isArray(self: Value) bool {
            const T = fields.ColumnType(self.Table, fields.fieldInfo(
                self.field_info,
                self.Table,
                self.name,
                self.field_context,
            ));

            return switch (@typeInfo(self.type)) {
                .pointer => |info| if (info.size == .Slice and info.child == T)
                    true
                else
                    false,
                else => false,
            };
        }

        pub fn isNull(self: Value) bool {
            return (self.type == @TypeOf(null));
        }
    };

    pub const Group = struct {
        name: []const u8,
        children: []const Node,
    };

    condition: Condition,
    value: Value,
    group: Group,

    pub fn render(
        self: Node,
        Adapter: type,
        comptime writer: anytype,
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
                const is_sequence = if (prev) |capture| capture == .value else false;
                const prefix = if (is_sequence) " AND " else "";
                if (value.type == @TypeOf(null)) {
                    writer.print("{s}{s}.{s} IS NULL", .{
                        prefix,
                        Adapter.identifier(value.Table.name),
                        Adapter.identifier(value.name),
                    }) catch unreachable;
                } else {
                    writer.print("{s}{s}.{s} = {s}", .{
                        prefix,
                        Adapter.identifier(value.Table.name),
                        Adapter.identifier(value.name),
                        if (value.isArray())
                            // XXX: This is PostgreSQL-specific - one day we'll need to figure
                            // out how to generate SQL for unknown (at comptime) array length.
                            // MySQL has `ANY` but it expects a subquery so maybe we'll need a
                            // temporary table or something equally horrible. SQLite doesn't have
                            // `ANY` at all so we may end up having to generate some parts of the
                            // SQL at runtime. :(
                            Adapter.anyParamSql(value.index)
                        else
                            Adapter.paramSql(value.index),
                    }) catch unreachable;
                }
            },
            .group => |group| {
                if (group.children.len > 1) writer.print("(", .{}) catch unreachable;
                var prev_child: ?Node = null;
                for (group.children) |child| {
                    if (prev_child) |capture| {
                        if (child == .group and (capture == .group or capture == .value)) {
                            writer.print(" AND ", .{}) catch unreachable;
                        }
                    }
                    child.render(Adapter, writer, depth + 1, prev_child);
                    prev_child = child;
                }
                if (group.children.len > 1) writer.print(")", .{}) catch unreachable;
            },
        }
    }

    fn ValuesTuple(comptime self: Node) type {
        var types: [self.countValues()]type = undefined;
        var index: usize = 0;

        switch (self) {
            .condition => {},
            .value => |value| {
                if (!value.isNull()) {
                    types[index.*] = value.ColumnType();
                    index.* += 1;
                }
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

    fn values_fields(comptime self: Node, Table: type, relations: []const type) [self.countValues()]Field {
        var fields_array: [self.countValues()]Field = undefined;
        var tuple_index: usize = 0;

        switch (self) {
            .condition => {},
            .value => |value| {
                if (!value.isNull()) {
                    fields_array[value.index] = .{
                        .Table = value.Table,
                        .name = value.name,
                        .context = value.field_context,
                        .column_type = value.ColumnType(),
                        .index = tuple_index,
                    };
                    tuple_index += 1;
                }
            },
            .group => |group| {
                appendFields(group, Table, relations, &fields_array, &tuple_index);
            },
        }

        return fields_array;
    }

    fn countValues(comptime self: Node) usize {
        var count: usize = 0;

        switch (self) {
            .condition => {},
            .value => |value| {
                if (!value.isNull()) count += 1;
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
                .value => |value| {
                    if (!value.isNull()) count.* += 1;
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
                    if (!value.isNull()) {
                        types[index.*] = value.ColumnType();
                        index.* += 1;
                    }
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
        comptime fields_array: []Field,
        tuple_index: *usize,
    ) void {
        for (group.children) |child| {
            switch (child) {
                .condition => {},
                .value => |value| {
                    if (!value.isNull()) {
                        fields_array[tuple_index.*] = .{
                            .Table = value.Table,
                            .name = value.name,
                            .context = value.field_context,
                            .column_type = value.ColumnType(),
                            .index = value.index,
                        };
                        tuple_index.* += 1;
                    }
                },
                .group => |capture| {
                    appendFields(capture, Table, relations, fields_array, tuple_index);
                },
            }
        }
    }
};

fn nodeTree(
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
                const nodes = childNodes(Table, relations, OG, field_info, info, path, field_context, value_index);
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
            .null => .{
                .value = .{
                    .field_context = field_context,
                    .Table = findRelation(Table, relations, path),
                    .name = name,
                    .type = T,
                    .field_info = field_info,
                    // We write `IS NULL` directly into the SQL without a bind param
                    .index = undefined,
                },
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

fn childNodes(
    Table: type,
    relations: []const type,
    OG: type,
    comptime field_info: std.builtin.Type.StructField,
    comptime struct_info: std.builtin.Type.Struct,
    comptime path: [][]const u8,
    comptime field_context: fields.FieldContext,
    comptime value_index: *usize,
) [struct_info.fields.len]Node {
    var nodes: [struct_info.fields.len]Node = undefined;

    for (struct_info.fields, 0..) |field, index| {
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

    return nodes;
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
    values_fields: []const Field,
    comptime tuple_index: usize,
) void {
    if (comptime coercion.canCoerceDelegate(@TypeOf(arg))) {
        assignValue(arg, ValuesTuple, values_tuple, ErrorsTuple, errors_tuple, values_fields, tuple_index);
        return;
    }

    switch (@typeInfo(@TypeOf(arg))) {
        .@"struct" => |info| {
            comptime var idx: usize = tuple_index;
            inline for (info.fields) |field| {
                assignValues(
                    @field(arg, field.name),
                    ValuesTuple,
                    values_tuple,
                    ErrorsTuple,
                    errors_tuple,
                    values_fields,
                    idx,
                );

                // TODO: There must be a better way of tracking current arg index - we can't pass a
                // comptime pointer to this function without triggering an error (runtime ref to
                // comptime var).
                const T = @TypeOf(@field(arg, field.name));
                idx += if (comptime coercion.canCoerceDelegate(T)) 1 else switch (@typeInfo(T)) {
                    .@"struct", .enum_literal => 0,
                    else => 1,
                };
            }
        },
        .enum_literal => {},
        .null => {},
        else => {
            assignValue(arg, ValuesTuple, values_tuple, ErrorsTuple, errors_tuple, values_fields, tuple_index);
        },
    }
}

fn assignValue(
    arg: anytype,
    ValuesTuple: type,
    values_tuple: *ValuesTuple,
    ErrorsTuple: type,
    errors_tuple: *ErrorsTuple,
    values_fields: []const Field,
    comptime tuple_index: isize,
) void {
    inline for (
        std.meta.fields(ValuesTuple),
        values_fields,
        0..,
    ) |field, value_field, index| {
        const tuple_field_name = std.fmt.comptimePrint("{d}", .{tuple_index});

        if (comptime tuple_index == index) {
            const field_info = comptime fields.fieldInfo(
                field,
                value_field.Table,
                value_field.name,
                value_field.context,
            );
            const coerced: coercion.CoercedValue(
                value_field.column_type,
                @TypeOf(arg),
            ) = coercion.coerce(
                value_field.Table,
                field_info,
                arg,
            );
            @field(values_tuple, tuple_field_name) = coerced.value;
            @field(errors_tuple, tuple_field_name) = coerced.err;
        }
    }
}

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
