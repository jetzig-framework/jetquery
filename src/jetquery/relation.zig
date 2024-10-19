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

pub fn RelationTable(Schema: type, Table: type, comptime name: RelationsEnum(Table)) type {
    const relation = @field(Table.relations, @tagName(name));
    const source = std.enums.nameCast(std.meta.DeclEnum(Schema), relation.relation_model_name);

    return @field(Schema, @tagName(source));
}

pub const JoinContext = enum { inner, outer, include };

pub fn Relation(
    Schema: type,
    Table: type,
    comptime name: RelationsEnum(Table),
    comptime columns: anytype,
    comptime join_context: JoinContext,
) type {
    comptime {
        const relation = @field(Table.relations, @tagName(name));
        return struct {
            pub const context = join_context;
            pub const Source = RelationTable(Schema, Table, name);
            pub const relation_type = relation.relation_type;
            pub const options = relation.options;
            pub const relation_name = @tagName(name);
            pub const select_columns = jetquery.columns.translate(Source, &.{}, columns);
            pub const primary_key = options.primary_key orelse "id";
            pub const foreign_key = options.foreign_key orelse switch (relation_type) {
                .belongs_to => relation_name ++ "_id",
                .has_many => null, // We have to infer this from the source table later.
            };
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

pub const HasManyOptions = struct {
    primary_key: ?[]const u8 = null,
    foreign_key: ?[]const u8 = null,
};

pub fn hasMany(comptime model_name: anytype, comptime has_many_options: HasManyOptions) type {
    return struct {
        pub const relation_model_name = @tagName(model_name);
        pub const relation_type: RelationType = .has_many;
        pub const options = has_many_options;
    };
}
