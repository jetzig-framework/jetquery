const std = @import("std");

pub const DistinctColumn = struct {
    name: []const u8,
    table: type,
};

pub fn translate(
    Table: type,
    relations: []const type,
    comptime args: anytype,
) [sizeOf(Table, relations, args)]DistinctColumn {
    comptime {
        var fields: [sizeOf(Table, relations, args)]DistinctColumn = undefined;
        var index: usize = 0;

        for (args) |arg| {
            switch (@typeInfo(@TypeOf(arg))) {
                .enum_literal => {
                    fields[index] = primaryDistinctColumn(Table, arg);
                    index += 1;
                },

                .@"struct" => {
                    const count = nestedDistinctColumns(relations, arg, undefined, true);
                    var buf: [count]DistinctColumn = undefined;
                    const nested = nestedDistinctColumns(relations, arg, &buf, false);
                    @memcpy(fields[index .. index + count], nested);
                    index += count;
                },
                else => |tag| {
                    @compileError(
                        "Expected union literal or struct in `distinct` arguments, found: `" ++ @tagName(tag) ++ "`",
                    );
                },
            }
        }

        return fields;
    }
}

fn sizeOf(
    Table: type,
    comptime relations: []const type,
    comptime args: anytype,
) usize {
    comptime {
        var size: usize = 0;
        for (args) |arg| {
            switch (@typeInfo(@TypeOf(arg))) {
                .enum_literal => {
                    _ = primaryDistinctColumn(Table, arg);
                    size += 1;
                },

                .@"struct" => {
                    size += nestedDistinctColumns(relations, arg, undefined, true);
                },
                else => |tag| {
                    @compileError(
                        "Expected union literal or struct in `distinct` arguments, found: `" ++ @tagName(tag) ++ "`",
                    );
                },
            }
        }
        return size;
    }
}

fn Union(
    Table: type,
    comptime columns: []const std.meta.FieldEnum(Table.Definition),
    relations: []const type,
) type {
    var enum_fields: []std.builtin.Type.EnumField = undefined;

    for (columns, 0..) |column, index| {
        enum_fields[index] = .{ .name = @tagName(column), .value = index };
    }

    for (relations, columns.len..) |Relation, index| {
        enum_fields[index] = .{ .name = Relation.relation_name, .value = index };
    }

    const Enum = @Type(.{
        .@"enum" = .{
            .tag_type = std.math.IntFittingRange(0, columns.len + relations.len),
            .fields = &enum_fields,
            .decls = &.{},
            .is_exhaustive = true,
        },
    });

    var union_fields: [columns.len + relations.len]std.builtin.Type.UnionField = undefined;

    for (columns, 0..) |column, index| {
        union_fields[index] = .{
            .name = @tagName(column),
            .type = void,
            .alignment = @alignOf(void),
        };
    }

    for (relations, columns.len..) |Relation, index| {
        union_fields[index] = .{
            .name = Relation.relation_name,
            .type = std.meta.FieldEnum(Relation.Source.Definition),
            .alignment = @alignOf(std.meta.FieldEnum(Relation.Source.Definition)),
        };
    }

    return @Type(.{
        .@"union" = .{
            .layout = .auto,
            .tag_type = Enum,
            .fields = &union_fields,
            .decls = &.{},
        },
    });
}

fn primaryDistinctColumn(
    Table: type,
    arg: anytype,
) DistinctColumn {
    comptime {
        for (Table.columns()) |column| {
            if (std.mem.eql(u8, @tagName(column), @tagName(arg))) return .{
                .table = Table,
                .name = @tagName(arg),
            };
        }
        @compileError(std.fmt.comptimePrint(
            "Failed matching distinct column `{s}` on `{s}`.",
            .{ @tagName(arg), Table.table_name },
        ));
    }
}

fn nestedDistinctColumns(
    relations: []const type,
    comptime arg: anytype,
    buf: []DistinctColumn,
    comptime count: bool,
) if (count) usize else []const DistinctColumn {
    var index: usize = 0;

    for (std.meta.fields(@TypeOf(arg))) |field| {
        for (relations) |Relation| {
            if (std.mem.eql(u8, field.name, Relation.relation_name)) {
                for (@field(arg, field.name)) |nested_arg| {
                    for (Relation.select_columns) |column| {
                        if (std.mem.eql(u8, @tagName(column), @tagName(nested_arg))) {
                            if (!count) {
                                buf[index] = .{ .name = @tagName(nested_arg), .table = Relation.Source };
                            }
                            index += 1;
                            break;
                        }
                    } else @compileError(std.fmt.comptimePrint(
                        "Failed matching `distinct` field `{s}.{s}` .",
                        .{ field.name, @tagName(nested_arg) },
                    ));
                }
                break;
            }
        } else @compileError(std.fmt.comptimePrint(
            "Failed matching relation `{s}` in `distinct` arguments.",
            .{field.name},
        ));
    }

    return if (count) index else buf[0..];
}
