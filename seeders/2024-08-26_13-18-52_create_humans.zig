const std = @import("std");
const jetquery = @import("jetquery");

pub fn run(repo: anytype) !void {
    try repo.insert(
        .Human,
        .{
            .id = 1,
            .name = "Curt Connors",
        },
    );
    try repo.insert(
        .Human,
        .{
            .id = 2,
            .name = "Dr. Evil",
        },
    );
    try repo.insert(
        .Human,
        .{
            .id = 3,
            .name = "Lady Tremaine",
        },
    );
}
