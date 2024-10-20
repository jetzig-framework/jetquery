const std = @import("std");

const jetcommon = @import("jetcommon");

const Where = @import("sql/Where.zig");

pub const FieldContext = enum { where, update, insert, limit, order, none };

pub const FieldInfo = struct {
    info: std.builtin.Type.StructField,
    name: []const u8,
    Table: type,
    context: FieldContext,
};

pub fn fieldInfos(
    Adapter: type,
    Table: type,
    relations: []const type,
    T: type,
    comptime context: FieldContext,
) [Where.tree(Adapter, Table, relations, T, context, 0).values_count]FieldInfo {
    comptime {
        const tree = Where.tree(Adapter, Table, relations, T, context, 0);
        var value_fields: [tree.values_count]FieldInfo = undefined;
        for (std.meta.fields(tree.ValuesTuple), tree.values_fields, 0..) |tuple_field, value_field, index| {
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
        const FT = fieldType(Table.Definition, field_info.name);
        if (FT == jetcommon.types.DateTime) return i64 else return FT;
    } else {
        // We only arrive here when we process triplets, e.g.
        // `.{ .foo, .lt_eql, 100 }`
        // But we coerce to the other side of the triplet and only use this type as a fallback in
        // the specific case that two values (i.e. not a column or SQL function) are used on both
        // sides of the triplet, e.g.:
        // `.{ 1, .lt, 100 }`
        // Without a know coercion target the only thing we can do here is use the value's type
        // and assume the database adapter will know what to do with it, otherwise we get a
        // compile error and the user has to do an explicit cast. This is all an edge case of an
        // edge case.
        return field_info.info.type;
    }
}

pub fn structField(comptime name: []const u8, T: type) std.builtin.Type.StructField {
    comptime {
        return .{
            .name = name ++ "",
            .type = T,
            .default_value = null,
            .is_comptime = false,
            .alignment = @alignOf(T),
        };
    }
}

pub fn structFieldDefault(comptime name: []const u8, default: anytype) std.builtin.Type.StructField {
    comptime {
        return .{
            .name = name ++ "",
            .type = @TypeOf(default),
            .default_value = &default,
            .is_comptime = false,
            .alignment = @alignOf(@TypeOf(default)),
        };
    }
}

pub fn structFieldComptime(
    comptime name: []const u8,
    comptime default: anytype,
) std.builtin.Type.StructField {
    comptime {
        return .{
            .name = name ++ "",
            .type = @TypeOf(default),
            .default_value = &default,
            .is_comptime = true,
            .alignment = @alignOf(@TypeOf(default)),
        };
    }
}

pub fn structType(comptime fields: []const std.builtin.Type.StructField) type {
    return @Type(.{
        .@"struct" = .{
            .layout = .auto,
            .fields = fields,
            .decls = &.{},
            .is_tuple = false,
        },
    });
}

pub fn fieldType(T: type, comptime name: []const u8) type {
    const tag = std.enums.nameCast(std.meta.FieldEnum(T), name);
    const FT = std.meta.fieldInfo(T, tag).type;
    return switch (@typeInfo(FT)) {
        .optional => |optional| optional.child,
        else => FT,
    };
}
