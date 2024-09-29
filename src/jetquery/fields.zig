const std = @import("std");

const jetcommon = @import("jetcommon");

pub const distinct = @import("fields/distinct.zig");

pub const FieldContext = enum { where, update, insert, limit, order, none };

pub const FieldInfo = struct {
    info: std.builtin.Type.StructField,
    name: []const u8,
    context: FieldContext,
};

pub fn fieldInfos(T: type, comptime context: FieldContext) [std.meta.fields(T).len]FieldInfo {
    var value_fields: [std.meta.fields(T).len]FieldInfo = undefined;
    for (std.meta.fields(T), 0..) |field, index| {
        value_fields[index] = fieldInfo(field, context);
    }
    return value_fields;
}

pub fn fieldInfo(comptime field: std.builtin.Type.StructField, comptime context: FieldContext) FieldInfo {
    return .{ .info = field, .context = context, .name = field.name };
}

pub fn FieldValues(Table: type, relations: []const type, comptime fields: []const FieldInfo) type {
    var new_fields: [fields.len]std.builtin.Type.StructField = undefined;
    for (fields, 0..) |field, index| {
        new_fields[index] = .{
            .name = std.fmt.comptimePrint("{}", .{index}),
            .type = ColumnType(Table, relations, field),
            .default_value = null,
            .is_comptime = false,
            .alignment = @alignOf(ColumnType(Table, relations, field)),
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
            .{ field_info.name, Table.table_name },
        ));
    }
}
