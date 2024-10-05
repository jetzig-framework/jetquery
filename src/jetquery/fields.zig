const std = @import("std");

const jetcommon = @import("jetcommon");

const jetquery = @import("../jetquery.zig");

pub const FieldContext = enum { where, update, insert, limit, order, none };

pub const FieldInfo = struct {
    info: std.builtin.Type.StructField,
    name: []const u8,
    Table: type,
    context: FieldContext,
};

pub fn fieldInfos(T: type, comptime context: FieldContext) [jetquery.Where.tree(T).context().len]FieldInfo {
    comptime {
        const tree = jetquery.Where.tree(T);
        const tree_context = tree.context();
        var value_fields: [tree_context.len]FieldInfo = undefined;
        for (std.meta.fields(tree_context.Tuple), tree_context.fields, 0..) |tuple_field, value_field, index| {
            value_fields[index] = fieldInfo(tuple_field, value_field.name, context);
        }
        return value_fields;
    }
}

pub fn fieldInfo(
    comptime field: std.builtin.Type.StructField,
    comptime name: []const u8,
    comptime context: FieldContext,
) FieldInfo {
    return .{ .info = field, .context = context, .name = name, .Table = undefined };
}

pub fn FieldValues(Table: type, relations: []const type, comptime fields: []const FieldInfo) type {
    _ = Table;
    _ = relations;
    var new_fields: [fields.len]std.builtin.Type.StructField = undefined;
    for (fields, 0..) |field, index| {
        new_fields[index] = .{
            .name = std.fmt.comptimePrint("{}", .{index}),
            // TODO: Move to Where
            // .type = ColumnType(Table, relations, field),
            .type = field.info.type,
            .default_value = null,
            .is_comptime = false,
            .alignment = @alignOf(field.info.type),
            // TODO: Move to Where
            // .alignment = @alignOf(ColumnType(Table, relations, field)),
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

pub fn ColumnType(Table: type, relations: []const type, comptime field_info: FieldInfo) type {
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
        for (relations) |Relation| {
            if (comptime @hasField(Relation.Source.Definition, field_info.name)) {
                const FT = std.meta.FieldType(
                    Relation.Source.Definition,
                    std.enums.nameCast(std.meta.FieldEnum(Relation.Source.Definition), field_info.name),
                );
                if (FT == jetcommon.types.DateTime) return i64 else return FT;
            }
        }

        @compileError(std.fmt.comptimePrint(
            "No column `{s}` defined in Schema for `{s}`.",
            .{ field_info.name, Table.name },
        ));
    }
}
