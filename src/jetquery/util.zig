pub inline fn stringMaybeEnum(arg: anytype) []const u8 {
    return switch (@typeInfo(@TypeOf(arg))) {
        .enum_literal => @tagName(arg),
        else => arg,
    };
}
