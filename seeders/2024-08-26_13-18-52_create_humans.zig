const std = @import("std");

pub fn run(repo: anytype) !void {
    try repo.insert(
        .Human,
        .{
            .name = "Curt Connors",
        },
    );
    try repo.insert(
        .Human,
        .{
            .name = "Dr. Evil",
        },
    );
    try repo.insert(
        .Human,
        .{
            .name = "Lady Tremaine",
        },
    );
}
