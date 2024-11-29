const std = @import("std");
const builtin = @import("builtin");

pub const CallerInfo = struct {
    debug_info: *std.debug.SelfInfo,
    file_name: []const u8,
    line_number: u64,

    pub fn deinit(self: CallerInfo) void {
        self.debug_info.allocator.free(self.file_name);
    }
};

pub fn getCallerInfo(mutex: *std.Thread.Mutex, address: usize) !?CallerInfo {
    if (comptime builtin.mode != .Debug) return null;

    mutex.lock();
    defer mutex.unlock();

    const debug_info = try std.debug.getSelfDebugInfo();
    const module = debug_info.getModuleForAddress(address) catch |err| switch (err) {
        error.MissingDebugInfo, error.InvalidDebugInfo => return null,
        else => return err,
    };

    const symbol_info = module.getSymbolAtAddress(debug_info.allocator, address) catch |err| switch (err) {
        error.MissingDebugInfo, error.InvalidDebugInfo => return null,
        else => return err,
    };

    return if (symbol_info.source_location) |source_location|
        .{
            .debug_info = debug_info,
            .file_name = source_location.file_name,
            .line_number = source_location.line,
        }
    else
        null;
}
