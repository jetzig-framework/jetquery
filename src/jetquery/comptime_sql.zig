const std = @import("std");

const jetquery = @import("../jetquery.zig");

pub fn renderSelect(Table: type, comptime adapter: jetquery.adapters.Adapter, comptime columns: []const std.meta.FieldEnum(Table.Definition), comptime limit: ?usize) []const u8 {
    comptime {
        const statement = "SELECT ";
        var columns_buf_len: usize = 0;
        for (columns, 0..) |column, index| {
            columns_buf_len += std.fmt.comptimePrint(
                "{}{s} ",
                .{ adapter.identifier(@tagName(column)), if (index + 1 < columns.len) "," else "" },
            ).len;
        }
        var columns_buf: [columns_buf_len]u8 = undefined;
        var cursor: usize = 0;
        for (columns, 0..) |column, index| {
            const column_identifier = std.fmt.comptimePrint(
                "{}{s} ",
                .{ adapter.identifier(@tagName(column)), if (index + 1 < columns.len) "," else "" },
            );
            @memcpy(columns_buf[cursor .. cursor + column_identifier.len], column_identifier);
            cursor += column_identifier.len;
        }
        const from = std.fmt.comptimePrint(" FROM {}", .{adapter.identifier(Table.table_name)});
        const limit_clause = if (limit) |bound| std.fmt.comptimePrint(" LIMIT {}", .{bound}) else "";
        return std.fmt.comptimePrint("{s}{s}{s}{s}", .{ statement, columns_buf, from, limit_clause });
    }
}
