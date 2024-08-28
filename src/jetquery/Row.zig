/// A row returned in a `Result`.
const std = @import("std");

const jetquery = @import("../jetquery.zig");

/// A row returned in a `Result`.
allocator: std.mem.Allocator,
values: []const jetquery.Value,
columns: [][]const u8,

const Row = @This();

pub fn deinit(self: Row) void {
    self.allocator.free(self.values);
}

/// Retrieve a typed value from a result row.
pub fn get(self: Row, T: type, column_name: []const u8) ?T {
    for (self.columns, self.values) |column, value| {
        if (std.mem.eql(u8, column_name, column)) return switch (T) {
            []const u8 => value.string,
            usize => value.integer,
            f64 => value.float,
            bool => value.boolean,
            else => @compileError("Unsupported type: " ++ @typeName(T)),
        };
    }
    return null;
}
