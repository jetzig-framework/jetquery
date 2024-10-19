const std = @import("std");

const fields = @import("../fields.zig");
const coercion = @import("../coercion.zig");
const columns = @import("../columns.zig");
const sql = @import("../sql.zig");

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
    Adapter: type,
    Table: type,
    relations: []const type,
    T: type,
    comptime field_context: fields.FieldContext,
    comptime first_value_index: usize,
) Tree {
    var index: usize = first_value_index;
    const root = nodeTree(
        Adapter,
        Table,
        relations,
        T,
        T,
        "root",
        undefined,
        &.{},
        field_context,
        &index,
    );

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
        synthetic: bool = false,

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

    pub const Triplet = struct {
        lhs: Operand,
        operator: Operator,
        rhs: Operand,

        pub const Operator = enum {
            eql,
            not_eql,
            lt,
            lt_eql,
            gt,
            gt_eql,
        };

        pub const Operand = union(enum) {
            value: Node.Value,
            column: columns.Column,
        };
    };

    condition: Condition,
    value: Value,
    group: Group,
    triplet: Triplet,

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
                        const is_and = switch (child) {
                            .group, .triplet => switch (capture) {
                                .group, .value, .triplet => true,
                                else => false,
                            },
                            else => false,
                        };
                        if (is_and) {
                            writer.print(" AND ", .{}) catch unreachable;
                        }
                    }
                    child.render(Adapter, writer, depth + 1, prev_child);
                    prev_child = child;
                }
                if (group.children.len > 1) writer.print(")", .{}) catch unreachable;
            },
            .triplet => |triplet| {
                switch (triplet.lhs) {
                    .value => |value| {
                        writer.print("{s}", .{Adapter.paramSql(value.index)}) catch unreachable;
                    },
                    .column => |column| {
                        writer.print("{s}", .{Adapter.columnSql(column.table, column)}) catch unreachable;
                    },
                }

                const operator = switch (triplet.operator) {
                    .eql => "=",
                    .not_eql => "<>",
                    .lt => "<",
                    .lt_eql => "<=",
                    .gt => ">",
                    .gt_eql => ">=",
                };

                writer.print(" {s} ", .{operator}) catch unreachable;

                switch (triplet.rhs) {
                    .value => |value| {
                        writer.print("{s}", .{Adapter.paramSql(value.index)}) catch unreachable;
                    },
                    .column => |column| {
                        writer.print("{s}", .{Adapter.columnSql(column.table, column)}) catch unreachable;
                    },
                }
            },
        }
    }

    fn ValuesTuple(comptime self: Node) type {
        const len = self.countValues();
        var types: [len]type = undefined;
        var index: usize = 0;

        appendValueType(self, len, &types, &index);
        return std.meta.Tuple(&types);
    }

    fn appendValueType(
        comptime node: Node,
        comptime len: usize,
        types: *[len]type,
        index: *usize,
    ) void {
        switch (node) {
            .condition => {},
            .value => |value| {
                if (!value.isNull()) {
                    types[index.*] = value.ColumnType();
                    index.* += 1;
                }
            },
            .group => |group| {
                for (group.children) |child| appendValueType(child, len, types, index);
            },
            .triplet => |triplet| {
                switch (triplet.lhs) {
                    .value => |value| {
                        if (!value.isNull()) {
                            types[index.*] = value.type;
                            index.* += 1;
                        }
                    },
                    .column => {},
                }
                switch (triplet.rhs) {
                    .value => |value| {
                        if (!value.isNull()) {
                            types[index.*] = value.type;
                            index.* += 1;
                        }
                    },
                    .column => {},
                }
            },
        }
    }

    fn ErrorsTuple(comptime self: Node) type {
        var types: [self.countValues()]type = undefined;
        for (0..self.countValues()) |index| {
            types[index] = ?anyerror;
        }

        return std.meta.Tuple(&types);
    }

    fn values_fields(comptime self: Node, Table: type, relations: []const type) [self.countValues()]Field {
        const len = self.countValues();
        var fields_array: [len]Field = undefined;
        var tuple_index: usize = 0;
        appendField(self, Table, relations, len, &fields_array, &tuple_index);

        return fields_array;
    }

    fn appendValueField(
        T: type,
        comptime value: Node.Value,
        comptime len: usize,
        fields_array: *[len]Field,
        tuple_index: *usize,
    ) void {
        if (!value.isNull()) {
            fields_array[tuple_index.*] = .{
                .Table = value.Table,
                .name = value.name,
                .context = value.field_context,
                .column_type = T,
                .index = tuple_index.*,
            };
            tuple_index.* += 1;
        }
    }

    fn appendField(
        node: Node,
        Table: type,
        relations: []const type,
        comptime len: usize,
        fields_array: *[len]Field,
        tuple_index: *usize,
    ) void {
        switch (node) {
            .condition => {},
            .value => |value| {
                appendValueField(value.ColumnType(), value, len, fields_array, tuple_index);
            },
            .group => |group| {
                for (group.children) |child| {
                    appendField(child, Table, relations, len, fields_array, tuple_index);
                }
            },
            .triplet => |triplet| {
                switch (triplet.lhs) {
                    .value => |value| {
                        appendValueField(value.type, value, len, fields_array, tuple_index);
                    },
                    .column => {},
                }
                switch (triplet.rhs) {
                    .value => |value| {
                        appendValueField(value.type, value, len, fields_array, tuple_index);
                    },
                    .column => {},
                }
            },
        }
    }

    fn countValues(comptime self: Node) usize {
        var count: usize = 0;
        countNodeValues(self, &count);

        return count;
    }

    fn countNodeValues(comptime node: Node, count: *usize) void {
        switch (node) {
            .condition => {},
            .value => |value| {
                if (!value.isNull()) count.* += 1;
            },
            .group => |group| {
                for (group.children) |child| countNodeValues(child, count);
            },
            .triplet => |triplet| {
                switch (triplet.lhs) {
                    .value => |value| {
                        if (!value.isNull()) count.* += 1;
                    },
                    .column => {},
                }
                switch (triplet.rhs) {
                    .value => |value| {
                        if (!value.isNull()) count.* += 1;
                    },
                    .column => {},
                }
            },
        }
    }
};

fn nodeTree(
    Adapter: type,
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
                if (isTriplet(T)) {
                    break :blk .{ .triplet = makeTriplet(
                        Adapter,
                        Table,
                        relations,
                        T,
                        field_context,
                        field_info,
                        name,
                        path,
                        value_index,
                    ) };
                }
                const nodes = childNodes(
                    Adapter,
                    Table,
                    relations,
                    OG,
                    field_info,
                    info,
                    path,
                    field_context,
                    value_index,
                );
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
    Adapter: type,
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
            Adapter,
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

    comptime var idx: usize = tuple_index;

    switch (@typeInfo(@TypeOf(arg))) {
        .@"struct" => |info| {
            // XXX: Note that we will always land here first because the first arg to `where` is
            // is always a struct, so we can safely start here for incrementing our index
            // counter.
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
                comptime detectValues(field.type, &idx);
            }
        },
        .type => {},
        .enum_literal => {},
        .null => {},
        else => {
            assignValue(
                arg,
                ValuesTuple,
                values_tuple,
                ErrorsTuple,
                errors_tuple,
                values_fields,
                tuple_index,
            );
        },
    }
}

fn detectValues(T: type, index: *usize) void {
    comptime {
        if (coercion.canCoerceDelegate(T)) {
            index.* += 1;
            return;
        }
        switch (@typeInfo(T)) {
            .@"struct" => |info| {
                for (info.fields) |field| detectValues(field.type, index);
            },
            .type, .enum_literal, .null => {},
            else => index.* += 1,
        }
    }
}

fn assignValue(
    arg: anytype,
    ValuesTuple: type,
    values_tuple: *ValuesTuple,
    ErrorsTuple: type,
    errors_tuple: *ErrorsTuple,
    values_fields: []const Field,
    comptime tuple_index: usize,
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

// A triplet with an operator, e.g.:
// ```zig
// .{ .foo, .eql, .bar }
// .{ sql.max(.foo), .lt, .bar }
fn isTriplet(T: type) bool {
    const struct_fields = std.meta.fields(T);

    if (struct_fields.len != 3) return false;
    if (@typeInfo(struct_fields[1].type) != .enum_literal) return false;

    const t: T = undefined;
    return @hasField(Node.Triplet.Operator, @tagName(t[1]));
}

fn makeTriplet(
    Adapter: type,
    Table: type,
    relations: []const type,
    T: type,
    comptime field_context: fields.FieldContext,
    comptime field_info: std.builtin.Type.StructField,
    comptime name: []const u8,
    comptime path: [][]const u8,
    comptime value_index: *usize,
) Node.Triplet {
    const arg: T = undefined;

    return .{
        .lhs = makeOperand(
            Adapter,
            Table,
            T,
            if (@TypeOf(arg[2]) == type) arg[2] else @TypeOf(arg[2]),
            0,
            2,
            relations,
            field_context,
            name,
            path,
            field_info,
            value_index,
        ),
        .operator = std.enums.nameCast(Node.Triplet.Operator, arg[1]),
        .rhs = makeOperand(
            Adapter,
            Table,
            T,
            if (@TypeOf(arg[0]) == type) arg[0] else @TypeOf(arg[0]),
            2,
            0,
            relations,
            field_context,
            name,
            path,
            field_info,
            value_index,
        ),
    };
}

fn makeOperand(
    Adapter: type,
    Table: type,
    T: type,
    Other: type,
    comptime arg_index: usize,
    comptime other_arg_index: usize,
    relations: []const type,
    comptime field_context: fields.FieldContext,
    comptime name: []const u8,
    comptime path: [][]const u8,
    comptime field_info: std.builtin.Type.StructField,
    comptime value_index: *usize,
) Node.Triplet.Operand {
    const arg: T = undefined;

    return switch (@typeInfo(@TypeOf(arg[arg_index]))) {
        .enum_literal => .{
            .column = columns.translate(Table, relations, .{arg[arg_index]})[0],
        },
        .type => functionColumn(arg[arg_index], Table, relations),
        else => blk: {
            const A = switch (@typeInfo(Other)) {
                .type => Adapter.Aggregate(functionColumn(Other).function.?),
                .enum_literal => enum_blk: {
                    const column = columns.translate(Table, relations, .{arg[other_arg_index]})[0];
                    break :enum_blk column.type;
                },
                // We're comparing two values, let Zig reconsile the types:
                else => switch (@typeInfo(@TypeOf(arg[arg_index]))) {
                    // We need to ensure that we don't store a comptime value otherwise our
                    // entire data structure needs to be comptime-known. This only occurs when a
                    // user does, `.where(.{ 1, .eql, 1 })` (e.g.), otherwise we use the other
                    // side of the triplet's type to coerce to.
                    .comptime_int => isize,
                    .comptime_float => f64,
                    else => @TypeOf(arg[arg_index]),
                },
            };
            const value: Node.Value = .{
                .field_context = field_context,
                .Table = findRelation(Table, relations, path),
                .name = name,
                .type = A,
                .field_info = field_info,
                .index = value_index.*,
            };
            value_index.* += 1;
            break :blk .{ .value = value };
        },
    };
}

fn functionColumn(T: type, Table: type, relations: []const type) Node.Triplet.Operand {
    return if (@hasField(T, "__jetquery_function"))
        .{ .column = columns.translate(
            Table,
            relations,
            .{T},
        )[0] }
    else
        @compileError("Unexpected type in clause: `" ++ @typeName(T) ++ "`");
}

fn debugNode(comptime node: Node, comptime depth: usize) void {
    const indent = " " ** depth;
    switch (node) {
        .condition => |value| {
            @compileLog(std.fmt.comptimePrint("{s}{s}", .{ indent, @tagName(value) }));
        },
        .value => |value| {
            @compileLog(std.fmt.comptimePrint("{s}{s}", .{ indent, value.name }));
        },
        .group => |group| {
            for (group.children) |child| {
                debugNode(child, depth + 1);
            }
        },
        .triplet => |triplet| {
            @compileLog(std.fmt.comptimePrint("{s}{s}{s}{s}", .{
                indent,
                @tagName(triplet.lhs),
                @tagName(triplet.operator),
                @tagName(triplet.rhs),
            }));
        },
    }
}

fn debug(value: anytype) void {
    @compileLog(std.fmt.comptimePrint("{any}", .{value}));
}
