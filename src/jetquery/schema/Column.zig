const table = @import("table.zig");

/// A database column.
pub const Column = @This();

name: []const u8,
type: Type,
options: Options = .{},
primary_key: bool = false,
timestamps: ?table.TimestampsOptions = null,

pub const Type = enum {
    string,
    integer,
    float,
    decimal,
    boolean,
    datetime,
    text,
    smallint,
    bigint,
    double_precision,
};

pub const OnDeleteOption = enum {
    CASCADE,
    NULL,
};

pub const Reference = [2][]const u8;

pub const ReferenceOptions = struct {
    on_delete: ?OnDeleteOption = null,
};

pub const Options = struct {
    optional: bool = false,
    index: bool = false,
    index_name: ?[]const u8 = null,
    unique: bool = false,
    reference: ?Reference = null,
    reference_options: ?ReferenceOptions = null,
    length: ?u16 = null,
    primary_key: bool = false,
};

pub fn init(
    comptime name: []const u8,
    comptime column_type: Type,
    comptime options: Options,
) Column {
    return .{ .name = name, .type = column_type, .options = options };
}
