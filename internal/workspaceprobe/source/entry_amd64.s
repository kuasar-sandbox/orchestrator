#include "textflag.h"

// Linux x86-64 process entry. argv[0] is our temporary executable, argv[1]
// its private directory, followed by the workspace mount targets.
TEXT ·entry(SB),NOSPLIT|NOFRAME,$0-0
	MOVQ SP, R12
	SUBQ $256, SP // struct statx
	MOVQ $1, R15
	CMPQ (R12), $2
	JL done
	MOVQ $87, AX // unlink(argv[0])
	MOVQ 8(R12), DI
	SYSCALL
	TESTQ AX, AX
	JNZ done
	MOVQ $84, AX // rmdir(argv[1])
	MOVQ 16(R12), DI
	SYSCALL
	TESTQ AX, AX
	JNZ done
	LEAQ 24(R12), R13
	XORQ R14, R14
next:
	MOVQ (R13), SI
	TESTQ SI, SI
	JZ success
retry:
	MOVQ $332, AX // statx(AT_FDCWD, path, AT_NO_AUTOMOUNT, STATX_TYPE, buf)
	MOVQ $-100, DI
	MOVQ $2048, DX
	MOVQ $1, R10
	MOVQ SP, R8
	SYSCALL
	CMPQ AX, $-4 // EINTR
	JE retry
	TESTQ AX, AX
	JZ complete
	// A failed earlier automount may supply its errno. Its token has been
	// released; repeating the metadata lookup now sees the idle trigger.
	CMPQ R14, $1
	JE done
	MOVQ $1, R14
	JMP retry
complete:
	MOVWQZX 28(SP), R9 // stx_mode must describe a directory
	ANDQ $0xf000, R9
	CMPQ R9, $0x4000
	JNE done
	XORQ R14, R14
	ADDQ $8, R13
	JMP next
success:
	XORQ R15, R15
done:
	MOVQ R15, DI
	MOVQ $60, AX // exit(status)
	SYSCALL
