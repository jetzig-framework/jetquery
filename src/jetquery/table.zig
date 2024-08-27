const jetquery = @import("../jetquery.zig");

pub fn column(name: []const u8, column_type: jetquery.Column.Type, options: struct {}) jetquery.Column {
    _ = options;
    return .{ .name = name, .type = column_type, .options = .{} };
}

pub fn primaryKey(name: []const u8, options: struct {}) jetquery.Column {
    _ = options;
    return .{ .name = name, .type = .integer, .options = .{}, .primary_key = true };
}

pub fn timestamps(options: struct {}) jetquery.Column {
    _ = options;
    return .{ .name = undefined, .type = undefined, .options = .{}, .timestamps = true };
}
