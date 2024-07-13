const std = @import("std");

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    const lib = b.addStaticLibrary(.{
        .name = "jetquery",
        .root_source_file = b.path("src/jetquery.zig"),
        .target = target,
        .optimize = optimize,
    });

    b.installArtifact(lib);

    const pg_module = b.dependency("pg", .{ .target = target, .optimize = optimize });
    lib.root_module.addImport("pg", pg_module.module("pg"));

    const lib_unit_tests = b.addTest(.{
        .root_source_file = b.path("src/jetquery.zig"),
        .target = target,
        .optimize = optimize,
    });
    lib_unit_tests.root_module.addImport("pg", pg_module.module("pg"));

    const run_lib_unit_tests = b.addRunArtifact(lib_unit_tests);

    const test_step = b.step("test", "Run unit tests");
    test_step.dependOn(&run_lib_unit_tests.step);
}
