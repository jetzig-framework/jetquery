/// A database column.
pub const Column = @This();

name: []const u8,
type: Type,
options: Options = .{},
primary_key: bool = false,
timestamps: bool = false,

pub const Type = enum { string, integer, float, decimal, boolean, datetime, text };
pub const Options = struct {
    not_null: bool = false,
    index: bool = false,
    index_name: ?[]const u8 = null,
    unique: bool = false,
};

pub fn init(name: []const u8, column_type: Type, options: Options) Column {
    return .{ .name = name, .type = column_type, .options = options };
}
