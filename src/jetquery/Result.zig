const std = @import("std");

const jetquery = @import("../jetquery.zig");

/// A result of an executed query.
pub const Result = union(enum) {
    postgresql: jetquery.adapters.PostgresqlAdapter.Result,

    pub fn deinit(self: *Result) void {
        switch (self.*) {
            inline else => |*adapted_result| adapted_result.deinit(),
        }
    }

    pub fn drain(self: *Result) !void {
        switch (self.*) {
            inline else => |*adapted_result| try adapted_result.drain(),
        }
    }

    pub fn next(self: *Result, query: anytype) !?@TypeOf(query).ResultType {
        return switch (self.*) {
            inline else => |*adapted_result| blk: {
                var row = try adapted_result.next(query) orelse break :blk null;

                self.extendInternalFields(@TypeOf(query), &row);
                break :blk row;
            },
        };
    }

    pub fn all(self: *Result, query: anytype) ![]const @TypeOf(query).ResultType {
        return switch (self.*) {
            inline else => |*adapted_result| try adapted_result.all(query),
        };
    }

    pub fn unary(self: *Result, T: type) !T {
        return switch (self.*) {
            inline else => |*adapted_result| try adapted_result.unary(T),
        };
    }

    fn extendInternalFields(self: *Result, Query: type, result: *Query.ResultType) void {
        result.__jetquery_id = switch (self.*) {
            inline else => |*adapted_result| adapted_result.repo.generateId(),
        };
        result.__jetquery_model = Query.info.Table;
        result.__jetquery_schema = Query.info.Schema;

        const T = Query.ResultType;

        inline for (std.meta.fields(T)) |field| {
            if (comptime !std.mem.startsWith(u8, field.name, jetquery.original_prefix)) {
                // TODO: Relation fields currently not created.
                if (@hasField(T, jetquery.original_prefix ++ field.name)) {
                    @field(result, jetquery.original_prefix ++ field.name) = @field(result, field.name);
                }
            }
        }
    }
};
