const std = @import("std");
const Allocator = std.mem.Allocator;
const ArrayList = std.ArrayList;

pub fn build(b: *std.Build) !void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    const lib = b.addLibrary(.{
        .name = "jetquery",
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/jetquery.zig"),
            .target = target,
            .optimize = optimize,
        }),
    });

    b.installArtifact(lib);

    const pg = b.dependency("pg", .{
        .target = target,
        .optimize = optimize,
        .openssl_lib_name = "ssl",
    }).module("pg");

    const jetcommon = b.dependency("jetcommon", .{
        .target = target,
        .optimize = optimize,
    }).module("jetcommon");

    lib.root_module.addImport("pg", pg);
    lib.root_module.addImport("jetcommon", jetcommon);

    const config_path = b.option(
        []const u8,
        "jetquery_config_path",
        "JetQuery configuration file path",
    ) orelse "jetquery.config.zig";

    const config = if (try fileExist(config_path))
        b.createModule(.{
            .root_source_file = .{
                .cwd_relative = config_path,
            },
        })
    else
        b.createModule(.{
            .root_source_file = b.path("src/default_config.zig"),
        });

    const jetquery = b.addModule("jetquery", .{
        .root_source_file = b.path("src/jetquery.zig"),
    });

    jetquery.addImport("pg", pg);
    jetquery.addImport("jetcommon", jetcommon);
    jetquery.addImport("jetquery.config", config);

    const migrations_path = b.option(
        []const u8,
        "jetquery_migrations_path",
        "Migrations path",
    ) orelse "migrations";

    const seeders_path = b.option(
        []const u8,
        "jetquery_seeders_path",
        "Seeders path",
    ) orelse "seeders";

    const test_filters = b.option(
        []const []const u8,
        "test-filter",
        "Skip tests that do not match any filter",
    ) orelse &[0][]const u8{};

    const lib_unit_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/jetquery.zig"),
            .target = target,
            .optimize = optimize,
        }),
        .filters = test_filters,
    });

    lib_unit_tests.root_module.addImport("pg", pg);
    lib_unit_tests.root_module.addImport("jetcommon", jetcommon);
    lib_unit_tests.root_module.addImport("jetquery.config", config);

    const run_lib_unit_tests = b.addRunArtifact(lib_unit_tests);

    const test_step = b.step("test", "Run unit tests");
    test_step.dependOn(&run_lib_unit_tests.step);

    const exe_generate_migrations = b.addExecutable(.{
        .name = "migrations",
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/generate_migrations.zig"),
            .target = target,
            .optimize = optimize,
        }),
    });

    const migration_unit_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/jetquery/Migrate.zig"),
            .target = target,
            .optimize = optimize,
        }),
        .filters = test_filters,
    });

    const run_migration_unit_tests = b.addRunArtifact(migration_unit_tests);
    test_step.dependOn(&run_migration_unit_tests.step);
    migration_unit_tests.step.dependOn(&exe_generate_migrations.step);

    const run_generate_migrations_cmd = b.addRunArtifact(exe_generate_migrations);
    const generated_migrations_path = run_generate_migrations_cmd.addOutputFileArg("migrations.zig");

    for (try findFilesSorted(b.allocator, migrations_path)) |path| {
        run_generate_migrations_cmd.addFileArg(.{ .cwd_relative = path });
    }

    const migrations = b.addModule(
        "jetquery_migrations",
        .{ .root_source_file = generated_migrations_path },
    );

    migrations.addImport("jetquery", jetquery);
    migrations.addImport("jetquery.config", config);
    migration_unit_tests.root_module.addImport("migrations", migrations);
    migration_unit_tests.root_module.addImport("jetquery", jetquery);
    migration_unit_tests.root_module.addImport("jetcommon", jetcommon);

    const exe_generate_seeder = b.addExecutable(.{
        .name = "seed",
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/generate_seeders.zig"),
            .target = target,
            .optimize = optimize,
        }),
    });

    const seed_unit_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/jetquery/Seed.zig"),
            .target = target,
            .optimize = optimize,
        }),
        .filters = test_filters,
    });

    const run_seed_unit_tests = b.addRunArtifact(seed_unit_tests);
    test_step.dependOn(&run_seed_unit_tests.step);
    seed_unit_tests.step.dependOn(&exe_generate_seeder.step);

    const run_generate_seeders_cmd = b.addRunArtifact(exe_generate_seeder);
    const generated_seeders_path = run_generate_seeders_cmd.addOutputFileArg("seeders.zig");

    for (try findFilesSorted(b.allocator, seeders_path)) |path| {
        run_generate_seeders_cmd.addFileArg(.{ .cwd_relative = path });
    }

    const jetquery_migrate_module = b.addModule(
        "jetquery_migrate",
        .{ .root_source_file = b.path("src/jetquery/Migrate.zig") },
    );

    jetquery_migrate_module.addImport("jetquery", jetquery);
    jetquery_migrate_module.addImport("migrations", migrations);
    jetquery_migrate_module.addImport("jetquery.config", config);
    jetquery_migrate_module.addImport("jetcommon", jetcommon);

    const seeders_module = b.addModule(
        "jetquery_seeders",
        .{ .root_source_file = generated_seeders_path },
    );

    seeders_module.addImport("jetquery", jetquery);
    seeders_module.addImport("jetquery.config", config);
    seed_unit_tests.root_module.addImport("migrations", migrations);
    seed_unit_tests.root_module.addImport("jetquery_migrate", jetquery_migrate_module);
    seed_unit_tests.root_module.addImport("seeders", seeders_module);
    seed_unit_tests.root_module.addImport("jetquery", jetquery);
    seed_unit_tests.root_module.addImport("jetcommon", jetcommon);

    const jetquery_seeder_module = b.addModule(
        "jetquery_seeder",
        .{ .root_source_file = b.path("src/jetquery/Seed.zig") },
    );

    jetquery_seeder_module.addImport("jetquery", jetquery);
    jetquery_seeder_module.addImport("seeders", seeders_module);
    jetquery_seeder_module.addImport("jetquery.config", config);
    jetquery_seeder_module.addImport("jetcommon", jetcommon);

    const jetquery_reflect_module = b.addModule(
        "jetquery_reflect",
        .{ .root_source_file = b.path("src/jetquery/reflection/Reflect.zig") },
    );

    jetquery_reflect_module.addImport("jetquery", jetquery);
    jetquery_reflect_module.addImport("migrations", migrations);
    jetquery_reflect_module.addImport("jetquery.config", config);
    jetquery_reflect_module.addImport("jetcommon", jetcommon);

    const reflect_unit_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/jetquery/reflection/Reflect.zig"),
            .target = target,
            .optimize = optimize,
        }),
        .filters = test_filters,
    });

    const run_reflect_unit_tests = b.addRunArtifact(reflect_unit_tests);
    reflect_unit_tests.root_module.addImport("jetquery", jetquery);
    reflect_unit_tests.root_module.addImport("jetcommon", jetcommon);
    test_step.dependOn(&run_reflect_unit_tests.step);
}

fn findFilesSorted(allocator: Allocator, path: []const u8) ![][]const u8 {
    const absolute_path = if (std.fs.path.isAbsolute(path))
        path
    else
        std.fs.cwd().realpathAlloc(allocator, path) catch |err| {
            switch (err) {
                error.FileNotFound => return &.{},
                else => return err,
            }
        };

    var dir = std.fs.openDirAbsolute(
        absolute_path,
        .{ .iterate = true },
    ) catch |err| {
        switch (err) {
            error.FileNotFound => return &.{},
            else => return err,
        }
    };
    defer dir.close();

    var files: ArrayList([]const u8) = .empty;
    defer files.deinit(allocator);

    var it = dir.iterate();
    while (try it.next()) |entry| {
        if (entry.kind != .file) continue;
        try files.append(
            allocator,
            try std.fs.path.join(allocator, &.{ absolute_path, entry.name }),
        );
    }

    std.mem.sort([]const u8, files.items, {}, cmpString);
    return files.toOwnedSlice(allocator);
}

fn cmpString(_: void, lhs: []const u8, rhs: []const u8) bool {
    return std.mem.order(u8, lhs, rhs).compare(.lt);
}

fn fileExist(path: []const u8) !bool {
    const file = std.fs.cwd().openFile(path, .{}) catch |err| {
        switch (err) {
            error.FileNotFound => return false,
            else => return err,
        }
    };

    file.close();

    return true;
}
