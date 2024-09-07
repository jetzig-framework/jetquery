/// A database identifier, e.g. table name, column name, etc.
pub const Identifier = @This();

name: []const u8,
quote_char: u8,

pub fn format(self: Identifier, actual_fmt: []const u8, options: anytype, writer: anytype) !void {
    // TODO: SQL injection
    _ = actual_fmt;
    _ = options;
    try writer.print("{0c}{1s}{0c}", .{ self.quote_char, self.name });
}
