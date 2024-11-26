# JetQuery

Database query library for [Jetzig](https://github.com/jetzig-framework/jetzig), a web framework written in [Zig](https://ziglang.org/).

Documentation: [https://www.jetzig.dev/documentation/sections/database/introduction](https://www.jetzig.dev/documentation/sections/database/introduction)

## Features

* Comptime SQL generation
* PostgreSQL adapter ([pg.zig](https://github.com/karlseguin/pg.zig))
* Powerful `WHERE` clause syntax
* Object Relational Mapper (ORM)
* Migrations
* Relations/Associations

Use [standalone](https://www.jetzig.dev/documentation/sections/database/standalone_usage) or with [Jetzig](https://www.jetzig.dev/).

```zig
const Schema = struct {
    pub const Cat = Model(
        @This(),
        "cats",
        struct {
            id: i32,
            name: []const u8,
            age: i32,
            favorite_sport: []const u8,
            status: []const u8,
        },
        .{ .relations = .{ .homes = hasMany(.Home, .{}) } },
    );

    pub const Home = Model(@This(), "homes", struct { id: i32, cat_id: i32, zip_code: []const u8 }, .{});
};

const query = Query(.postgresql, Schema, .Cat)
    .join(.inner, .homes)
    .where(.{
        .{ .name = "Hercules" }, .OR, .{ .name = "Heracles" },
        .{ .{ .age, .gt, 4 }, .{ .age, .lt, 10 } },
        .{ .favorite_sport, .like, "%ball" },
        .{ .favorite_sport, .not_eql, "basketball" },
        .{ "my_sql_function(age)", .eql, 100 },
        .{ .NOT, .{ .{ .age = 1 }, .OR, .{ .age = 2 } } },
        .{ "age / paws = ? or age * paws < ?", .{ 2, 10 } },
        .{ .{ .status = null }, .OR, .{ .status = [_][]const u8{ "sleeping", "eating" } } },
        .{ .homes = .{ .zip_code = "10304" } },
    });

var repo = try Repo(.postgresql, Schema).init(std.testing.allocator, .{
    .adapter = .{
        .database = "example_database",
        .hostname = "127.0.0.1",
        .port = 5432,
        .username = "postgres",
        .password = "password",
    },
});

for (try repo.all(query)) |cat| {
    std.debug.print("{s} lives in these ZIP codes:\n", .{cat.name});

    for (cat.homes) |home| {
        std.debug.print("{s}\n", .{home.zip_code});
    }
}
```

## Testing

Use the provided _Docker Compose_ configuration to launch a local test database:

```console
docker compose up
```

Run tests:

```console
zig build test
```
