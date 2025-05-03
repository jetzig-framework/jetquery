const std = @import("std");

pub fn run(repo: anytype) !void {
    try repo.insert(
        .Cat,
        .{
            .name = "Mr. Bigglesworth",
            .paws = 4,
            .human_id = 2,
        },
    );
    try repo.insert(
        .Cat,
        .{
            .name = "Lucifer",
            .paws = 4,
            .human_id = 3,
        },
    );
}
