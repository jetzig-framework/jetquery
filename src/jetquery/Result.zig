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
            inline else => |*adapted_result| try adapted_result.next(query),
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
};
