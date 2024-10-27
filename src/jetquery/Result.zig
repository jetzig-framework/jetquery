const std = @import("std");

const jetquery = @import("../jetquery.zig");
const AuxiliaryQuery = @import("Query.zig").AuxiliaryQuery;

/// A result of an executed query.
pub fn Result(AdaptedRepo: type) type {
    return union(enum) {
        postgresql: jetquery.adapters.PostgresqlAdapter.Result(AdaptedRepo),

        const Self = @This();

        pub fn deinit(self: *Self) void {
            switch (self.*) {
                inline else => |*adapted_result| adapted_result.deinit(),
            }
        }

        pub fn drain(self: *Self) !void {
            switch (self.*) {
                inline else => |*adapted_result| try adapted_result.drain(),
            }
        }

        pub fn next(self: *Self, query: anytype) !?@TypeOf(query).ResultType {
            const ResultType = @TypeOf(query).ResultType;

            return switch (self.*) {
                inline else => |*adapted_result| blk: {
                    var row = try adapted_result.next(query) orelse break :blk null;

                    extendInternalFields(@TypeOf(query), &row);

                    const primary_key = @TypeOf(query).info.Table.primary_key;
                    const primary_key_present = @hasField(
                        @TypeOf(query).info.Table.Definition,
                        primary_key,
                    );

                    // Create a secondary connection for fetching relations if needed. This allows us
                    // to continue iterating over the primary query and fetching relations on each
                    // iteration. Since these connections are backed by a pool (in pg.zig) we should
                    // be okay acquiring a new connection for each call to `next()`.
                    var connection = if (query.auxiliary_queries.len > 0)
                        try adapted_result.repo.connect()
                    else {};
                    defer if (query.auxiliary_queries.len > 0) connection.release();

                    inline for (query.auxiliary_queries) |aux_query| {
                        const foreign_key = comptime aux_query.relation.foreign_key orelse
                            @TypeOf(query).info.Table.defaultForeignKey();

                        const Args = WhereArgs(aux_query, foreign_key, .one);
                        var args: Args = undefined;

                        const q = if (comptime primary_key_present) q_blk: {
                            @field(args, foreign_key) = @field(row, primary_key);
                            break :q_blk aux_query.baseQuery().where(args);
                        } else @compileError(std.fmt.comptimePrint(
                            "Unable to fetch relation records for `{s}` without primary key.",
                            .{aux_query.relation.relation_name},
                        ));

                        var aux_result = try connection.execute(
                            q,
                            adapted_result.caller_info,
                            adapted_result.repo,
                        );
                        defer aux_result.deinit();

                        const aux_type = AuxType(ResultType, aux_query.relation);

                        var aux_rows = std.ArrayList(aux_type).init(adapted_result.allocator);
                        while (try aux_result.next(q)) |aux_row| {
                            try aux_rows.append(mergeAux(
                                aux_type,
                                q,
                                @TypeOf(aux_row),
                                aux_row,
                            ));
                        }
                        @field(row, aux_query.relation.relation_name) = try aux_rows.toOwnedSlice();
                    }
                    break :blk row;
                },
            };
        }

        pub fn all(self: *Self, query: anytype) ![]@TypeOf(query).ResultType {
            const ResultType = @TypeOf(query).ResultType;

            return switch (self.*) {
                inline else => |*adapted_result| blk: {
                    var rows = try adapted_result.all(query);
                    const MergedRow = MergedRowType(query.auxiliary_queries, ResultType);
                    const primary_key = @TypeOf(query).info.Table.primary_key;
                    const primary_key_present = @hasField(
                        @TypeOf(query).info.Table.Definition,
                        primary_key,
                    );

                    var map = Map(@TypeOf(query), MergedRow, primary_key)
                        .init(adapted_result.allocator);
                    defer map.deinit();

                    for (rows, 0..) |row, index| {
                        if (comptime primary_key_present) {
                            const id = @field(row, primary_key);
                            try map.id_map.put(id, index);
                            try map.id_array.append(id);
                        }
                        var adapted_row = row;
                        extendInternalFields(@TypeOf(query), &adapted_row);

                        var merged_row: MergedRow = undefined;
                        inline for (query.auxiliary_queries) |init_aux_query| {
                            const aux_type = AuxType(ResultType, init_aux_query.relation);
                            @field(
                                merged_row,
                                init_aux_query.relation.relation_name,
                            ) = std.ArrayList(aux_type).init(adapted_result.allocator);
                        }
                        const aux_values = try map.aux_map.getOrPut(index);
                        aux_values.value_ptr.* = merged_row;
                        rows[index] = adapted_row;
                    }

                    // Execute secondary queries (hasMany relations when `include` is used) where
                    // foreign keys match the primary keys returned by the primary query, then merge
                    // the results together.
                    inline for (query.auxiliary_queries) |aux_query| {
                        const foreign_key = comptime aux_query.relation.foreign_key orelse
                            @TypeOf(query).info.Table.defaultForeignKey();

                        const Args = WhereArgs(aux_query, foreign_key, .many);
                        var args: Args = undefined;

                        const q = if (comptime primary_key_present) q_blk: {
                            @field(args, foreign_key) = map.id_array.items;
                            break :q_blk aux_query.baseQuery().where(args);
                        } else @compileError(std.fmt.comptimePrint(
                            "Unable to fetch relation records for `{s}` without primary key.",
                            .{aux_query.relation.relation_name},
                        ));

                        var aux_result = try adapted_result.repo.executeInternal(
                            q,
                            adapted_result.caller_info,
                        );
                        defer aux_result.deinit();

                        const aux_type = AuxType(ResultType, aux_query.relation);

                        while (try aux_result.next(q)) |aux_row| {
                            const adapted_aux_row = mergeAux(
                                aux_type,
                                q,
                                @TypeOf(aux_row),
                                aux_row,
                            );

                            if (comptime primary_key_present) try mapAux(
                                aux_query,
                                aux_type,
                                adapted_aux_row,
                                @TypeOf(aux_row),
                                aux_row,
                                foreign_key,
                                @TypeOf(map),
                                &map,
                            );
                        }
                        try aux_result.drain();
                    }

                    var it = map.aux_map.iterator();
                    while (it.next()) |entry| {
                        inline for (std.meta.fields(@TypeOf(entry.value_ptr.*))) |field| {
                            @field(rows[entry.key_ptr.*], field.name) = try @field(
                                entry.value_ptr.*,
                                field.name,
                            ).toOwnedSlice();
                        }
                    }

                    break :blk rows;
                },
            };
        }

        pub fn unary(self: *Self, T: type) !T {
            return switch (self.*) {
                inline else => |*adapted_result| try adapted_result.unary(T),
            };
        }

        fn extendInternalFields(Query: type, result: *Query.ResultType) void {
            result.__jetquery_model = Query.info.Table;
            result.__jetquery_schema = Query.info.Schema;

            const originals = std.meta.fields(@TypeOf(result.__jetquery.original_values));

            inline for (originals) |field| {
                @field(result.__jetquery.original_values, field.name) = @field(result, field.name);
            }

            inline for (Query.relations) |relation| {
                if (comptime relation.relation_type != .belongs_to) continue;

                inline for (relation.select_columns) |select_column| {
                    const relation_field = @field(result, relation.relation_name);
                    const value = @field(relation_field, select_column.name);
                    @field(
                        @field(result, relation.relation_name).__jetquery.original_values,
                        select_column.name,
                    ) = value;
                }
            }
        }

        pub inline fn duration(self: Result) i64 {
            return switch (self) {
                inline else => |adapted_result| adapted_result.duration,
            };
        }

        fn mergeAux(aux_type: type, q: anytype, T: type, aux_row: T) aux_type {
            var extended_aux_row = aux_row;
            extendInternalFields(@TypeOf(q), &extended_aux_row);

            var adapted_aux_row: aux_type = undefined;

            inline for (std.meta.fields(aux_type)) |field| {
                if (comptime std.mem.startsWith(u8, field.name, "__jetquery")) continue;
                @field(adapted_aux_row, field.name) = @field(aux_row, field.name);
                @field(
                    adapted_aux_row.__jetquery.original_values,
                    field.name,
                ) = @field(aux_row, field.name);
            }
            return adapted_aux_row;
        }

        fn mapAux(
            aux_query: anytype,
            aux_type: type,
            adapted_aux_row: aux_type,
            T: type,
            aux_row: T,
            comptime foreign_key: []const u8,
            MapType: type,
            map: *MapType,
        ) !void {
            const foreign_key_value = @field(aux_row, foreign_key);
            const maybe_row_index = switch (@typeInfo(@TypeOf(foreign_key_value))) {
                .optional => if (foreign_key_value) |value|
                    map.id_map.get(value)
                else
                    null,
                else => map.id_map.get(foreign_key_value),
            };

            if (maybe_row_index) |row_index| {
                // We pre-fill the map with an empty `MergedRow` so this is
                // guaranteed to exist (or we have a bug).
                const aux_values = map.aux_map.getEntry(row_index).?;
                try @field(
                    aux_values.value_ptr.*,
                    aux_query.relation.relation_name,
                ).append(adapted_aux_row);
            }
        }
    };
}

fn MergedRowType(auxiliary_queries: []const AuxiliaryQuery, ResultType: type) type {
    var fields: [auxiliary_queries.len]std.builtin.Type.StructField = undefined;
    for (auxiliary_queries, 0..) |aux_query, index| {
        fields[index] = jetquery.fields.structField(
            aux_query.relation.relation_name,
            std.ArrayList(AuxType(ResultType, aux_query.relation)),
        );
    }
    return jetquery.fields.structType(&fields);
}

fn AuxType(ResultType: type, Relation: type) type {
    const field_name = std.enums.nameCast(
        std.meta.FieldEnum(ResultType),
        Relation.relation_name,
    );
    return switch (@typeInfo(std.meta.fieldInfo(ResultType, field_name).type)) {
        .pointer => |info| info.child,
        inline else => |tag| @compileError(std.fmt.comptimePrint(
            "Expected slice for relation, found: `{s}`",
            .{@tagName(tag) ++ "`"},
        )),
    };
}

pub const AuxiliaryResult = struct {
    result: jetquery.Result,
    relation: type,
};

fn WhereArgs(
    aux_query: AuxiliaryQuery,
    comptime foreign_key: []const u8,
    arg_context: enum { one, many },
) type {
    const field_type = jetquery.fields.fieldType(
        aux_query.relation.Source.Definition,
        foreign_key,
    );
    comptime {
        var fields: [1]std.builtin.Type.StructField = .{jetquery.fields.structField(
            foreign_key,
            switch (arg_context) {
                .one => field_type,
                .many => []const field_type,
            },
        )};
        return jetquery.fields.structType(&fields);
    }
}

fn IdMap(Query: type, comptime primary_key: []const u8) type {
    const PK = if (comptime @hasField(Query.info.Table.Definition, primary_key))
        jetquery.fields.fieldType(Query.info.Table.Definition, primary_key)
    else
        void;
    return switch (PK) {
        []const u8 => std.StringHashMap(usize),
        else => std.AutoHashMap(PK, usize),
    };
}

fn PrimaryKey(Query: type, comptime primary_key: []const u8) type {
    return if (comptime @hasField(Query.info.Table.Definition, primary_key))
        jetquery.fields.fieldType(Query.info.Table.Definition, primary_key)
    else
        void;
}

fn Map(QueryType: type, MergedRow: type, comptime primary_key: []const u8) type {
    return struct {
        id_array: std.ArrayList(PrimaryKey(QueryType, primary_key)),
        id_map: IdMap(QueryType, primary_key),
        aux_map: std.AutoHashMap(usize, MergedRow),

        pub fn init(allocator: std.mem.Allocator) @This() {
            const PK = PrimaryKey(QueryType, primary_key);
            const IM = IdMap(QueryType, primary_key);
            const AM = std.AutoHashMap(usize, MergedRow);
            return .{
                .id_array = std.ArrayList(PK).init(allocator),
                .id_map = IM.init(allocator),
                .aux_map = AM.init(allocator),
            };
        }

        pub fn deinit(self: *@This()) void {
            defer self.id_array.deinit();
            defer self.id_map.deinit();
            defer self.aux_map.deinit();
        }
    };
}
