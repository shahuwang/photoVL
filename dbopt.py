import lancedb
import argparse
import sys

def get_table(table_name):
    """连接数据库并打开表"""
    try:
        db = lancedb.connect("photoVL_lancedb")
        return table.open_table(table_name) if table_name in db.table_names() else None
    except Exception as e:
        print(f"数据库连接错误: {e}")
        sys.exit(1)

def print_all_rows(table_name):
    """输出所有行数据"""
    db = lancedb.connect("photoVL_lancedb")
    if table_name not in db.table_names():
        print(f"错误: 表 '{table_name}' 不存在。")
        return

    table = db.open_table(table_name)
    # 使用 to_list() 获取字典格式，方便按键值对输出
    data = table.search().to_list()
    
    print(f"\n=== 表 '{table_name}' 数据详情 (共 {len(data)} 条) ===")
    for i, row in enumerate(data):
        print(f"\n[Row {i+1}]")
        for k, v in row.items():
            # 跳过向量字段，避免刷屏
            if k in ("vector", "image_vector", "face_vector"):
                display_v = f"[{type(v).__name__}] (向量数据已省略)"
            else:
                display_v = v
            print(f"  {k}: {display_v}")

def delete_all_rows(table_name):
    """删除所有行数据"""
    db = lancedb.connect("photoVL_lancedb")
    if table_name not in db.table_names():
        print(f"错误: 表 '{table_name}' 不存在。")
        return

    table = db.open_table(table_name)
    confirm = input(f"确定要清空表 '{table_name}' 吗？此操作不可逆！(y/n): ")
    if confirm.lower() == 'y':
        table.delete("true")
        # 立即回收磁盘空间
        table.cleanup_old_versions()
        print(f"已清空表 '{table_name}' 的所有数据。")
    else:
        print("操作已取消。")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="LanceDB 管理工具")
    parser.add_argument("--table", type=str, default="image_metadata", help="指定操作的表名")
    parser.add_argument("--action", type=str, choices=["show", "clear"], required=True, 
                        help="执行的操作: show (展示数据) 或 clear (清空数据)")

    args = parser.parse_args()

    if args.action == "show":
        print_all_rows(args.table)
    elif args.action == "clear":
        delete_all_rows(args.table)
