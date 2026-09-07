# Proxy worker 生命周期与转发取消

[English](proxy-worker-lifetime.md)

Proxy worker 是专用、单次运行的子进程。内置入口及定制 App 在 `Run` 返回后必须退出该进程；它们不是供常驻进程在内部反复重启 worker 的 API。

成功建立的 route SHM 和 admission arena 映射由 worker 进程拥有。`PreparedWorker.Close` 关闭继承的描述符，但不卸载这两份映射。`Run` 返回时，已有 handler、扩展和 traffic GC 仍可能持有引用。回收进程级映射不需要所有权标志、引用计数、优雅排空协议或等待 `http.Server.Shutdown`；内核在子进程退出时统一回收。映射构造函数仍清理自己尚未成功返回的操作；底层映射测试只有在所有读者退出后才可显式卸载。worker 生命周期测试使用子进程，不让生产退出路径为测试中的进程内复用增加机制。

这不改变正常请求的清理。请求或完整隧道结束时仍关闭 backend，并恰好释放一次 inflight 贡献。supervisor 仍必须等待进程退出，才清理该 worker 的共享计数和统计贡献：卸载一个进程的映射不会清零其他进程仍使用的共享内存。master route apply 和 Create barrier 不等待 worker 健康或 handler 退出。

流量限制只决定是否接纳新 flow，不决定已接纳 flow 的转发行为。普通 HTTP 请求取消和 HTTP/2 CONNECT stream 取消，无论是否配置有效限制，都关闭对应 backend。HTTP/1 hijacked CONNECT 保持半关闭语义，原 HTTP request context 不是通用的隧道关闭信号。native exec 保持既有 KAT、首帧、CEL、traffic admission 顺序。

本修正不新增 worker 优雅排空、连接池、全局限流或公开配置，也不修改内部 wire/layout。
