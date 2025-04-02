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

pub fn concatRelations(
    relations: []const type,
    Schema: type,
    Table: type,
    comptime name: RelationsEnum(Table),
    comptime relation_options: RelationOptions,
    comptime join_context: JoinContext,
) *const [1 + relations.len]type {
    comptime {
        var types: [1 + relations.len]type = undefined;
        for (relations, 0..) |relation, index| types[index] = relation;
        types[relations.len] = Relation(
            Schema,
            Table,
            name,
            relation_options,
            join_context,
        );
        const final = types;
        return &final;
    }
}

pub fn Relation(
    Schema: type,
    Table: type,
    comptime name: RelationsEnum(Table),
    comptime relation_options: RelationOptions,
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
            pub const select_columns = relation_options.select;
            pub const limit = relation_options.limit;
            pub const order_by = relation_options.order_by;
            pub const primary_key = options.primary_key orelse "id";
            pub const foreign_key: ?[]const u8 = options.foreign_key orelse switch (relation_type) {
                .belongs_to => relation_name ++ "_id",
                .has_many => null, // We have to infer this from the source table later.
            };
        };
    }
}

pub const RelationOptions = struct {
    select: []const jetquery.columns.Column = &.{},
    limit: ?u64 = null,
    order_by: ?[]const jetquery.sql.OrderClause = &.{},
};

pub fn translateRelationOptions(
    Schema: type,
    Model: type,
    comptime name: RelationsEnum(Model),
    comptime options: anytype,
) RelationOptions {
    var translated: RelationOptions = .{};

    const Source = RelationTable(Schema, Model, name);

    translated.select = &jetquery.columns.translate(
        Source,
        &.{},
        if (@hasField(@TypeOf(options), "select")) options.select else .{},
        .{ .from = @tagName(name) },
    );

    translated.limit = if (@hasField(@TypeOf(options), "limit"))
        options.limit
    else
        null;

    translated.order_by = if (@hasField(@TypeOf(options), "order_by"))
        &jetquery.sql.translateOrderBy(Source, &.{}, options.order_by)
    else
        null;

    return translated;
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

fn detectLimit(options: anytype, relation_type: RelationType) ?u64 {
    if (@hasField(@TypeOf(options), "limit")) {
        if (relation_type != .has_many) {
            @compileError(
                "`limit` on `include` only supported for `has_many` relations, found: `" ++
                    @tagName(relation_type) ++ "`",
            );
        } else return @intCast(options.limit);
    } else return null;
}
