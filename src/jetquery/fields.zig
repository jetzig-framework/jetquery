const std = @import("std");

const jetcommon = @import("jetcommon");

const Where = @import("Where.zig");

pub const FieldContext = enum { where, update, insert, limit, order, none };

pub const FieldInfo = struct {
    info: std.builtin.Type.StructField,
    name: []const u8,
    Table: type,
    context: FieldContext,
};

pub fn fieldInfos(
    Table: type,
    relations: []const type,
    T: type,
    comptime context: FieldContext,
) [Where.tree(Table, relations, T, context).context(Table, relations, 0).len]FieldInfo {
    comptime {
        const tree = Where.tree(Table, relations, T, context);
        const tree_context = tree.context(Table, relations, 0);
        var value_fields: [tree_context.len]FieldInfo = undefined;
        for (
            std.meta.fields(tree_context.ValuesTuple),
            tree_context.fields,
            0..,
        ) |tuple_field, value_field, index| {
            value_fields[index] = fieldInfo(tuple_field, value_field.Table, value_field.name, context);
        }
        return value_fields;
    }
}

pub fn fieldInfo(
    comptime field: std.builtin.Type.StructField,
    Table: type,
    comptime name: []const u8,
    comptime context: FieldContext,
) FieldInfo {
    return .{ .info = field, .context = context, .name = name, .Table = Table };
}

pub fn FieldValues(Table: type, relations: []const type, comptime fields: []const FieldInfo) type {
    _ = Table;
    _ = relations;
    var new_fields: [fields.len]std.builtin.Type.StructField = undefined;
    for (fields, 0..) |field, index| {
        new_fields[index] = .{
            .name = std.fmt.comptimePrint("{}", .{index}),
            .type = field.info.type,
            .default_value = null,
            .is_comptime = false,
            .alignment = @alignOf(field.info.type),
        };
    }
    return @Type(.{
        .@"struct" = .{
            .layout = .auto,
            .fields = &new_fields,
            .decls = &.{},
            .is_tuple = true,
        },
    });
}

pub fn ColumnType(Table: type, comptime field_info: FieldInfo) type {
    switch (field_info.context) {
        .limit => return usize,
        else => {},
    }

    if (comptime @hasField(Table.Definition, field_info.name)) {
        const FT = std.meta.FieldType(
            Table.Definition,
            std.enums.nameCast(std.meta.FieldEnum(Table.Definition), field_info.name),
        );
        if (FT == jetcommon.types.DateTime) return i64 else return FT;
    } else {
        @compileError(std.fmt.comptimePrint(
            "No column `{s}` defined in Schema for `{s}`.",
            .{ field_info.name, Table.name },
        ));
    }
}
