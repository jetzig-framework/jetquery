const std = @import("std");

const jetquery = @import("../../jetquery.zig");

const PostgresqlAdapter = @This();

database: []const u8,
username: []const u8,
password: []const u8,
hostname: []const u8,
port: u16 = 5432,

pub fn execute(self: PostgresqlAdapter, sql: []const u8) !jetquery.Result {
    _ = self;
    _ = sql;
    return .{};
}

// var pool = try pg.Pool.init(allocator, .{
//   .size = 5,
//   .connect = .{
//     .port = 5432,
//     .host = "127.0.0.1",
//   },
//   .auth = .{
//     .username = "postgres",
//     .database = "postgres",
//     .password = "root_pw",
//     .timeout = 10_000,
//   }
// });
// defer pool.deinit();
//
// var result = try pool.query("select id, name from users where power > $1", .{9000});
// defer result.deinit();
//
// while (try result.next()) |row| {
//   const id = row.get(i32, 0);
//   // this is only valid until the next call to next(), deinit() or drain()
//   const name = row.get([]u8, 1);
// }
