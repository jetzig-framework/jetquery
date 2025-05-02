const std = @import("std");
const jetquery = @import("jetquery");
const t = jetquery.schema.table;

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
