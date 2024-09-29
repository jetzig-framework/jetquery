const std = @import("std");

const jetquery = @import("../jetquery.zig");

pub fn RelationsEnum(Table: type) type {
    return std.meta.FieldEnum(@TypeOf(Table.relations));
}

pub fn ColumnsEnum(Schema: type, Table: type, comptime name: RelationsEnum(Table)) type {
    comptime {
        const relation = @field(Table.relations, @tagName(name));
        const source = std.enums.nameCast(std.meta.DeclEnum(Schema), relation.relation_model_name);
        const Source = @field(Schema, @tagName(source));
        return std.meta.FieldEnum(Source.Definition);
    }
}

pub fn Relation(
    Schema: type,
    Table: type,
    comptime name: RelationsEnum(Table),
    columns: []const ColumnsEnum(Schema, Table, name),
) type {
    comptime {
        const relation = @field(Table.relations, @tagName(name));
        const source = std.enums.nameCast(std.meta.DeclEnum(Schema), relation.relation_model_name);
        return struct {
            pub const Source = @field(Schema, @tagName(source));
            pub const relation_type = relation.relation_type;
            pub const options = relation.options;
            pub const relation_name = @tagName(name);
            pub const select_columns = columns;
        };
    }
}

pub const RelationType = enum { belongs_to, has_many };
pub const BelongsToOptions = struct {
    primary_key: ?[]const u8 = null,
    foreign_key: ?[]const u8 = null,
};

pub fn belongsTo(comptime model_name: anytype, comptime belongs_to_options: BelongsToOptions) type {
    return struct {
        pub const relation_model_name = @tagName(model_name);
        pub const relation_type: RelationType = .belongs_to;
        pub const options = belongs_to_options;
    };
}

// TODO
pub const hasMany = belongsTo;
