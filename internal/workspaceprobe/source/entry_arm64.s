#include "textflag.h"

// Linux arm64 process entry. See entry_amd64.s for its argv contract.
TEXT ·entry(SB),NOSPLIT|NOFRAME,$0-0
	MOVD RSP, R19
	SUB $256, RSP // struct statx
	MOVD $1, R24
	MOVD (R19), R4
	CMP $2, R4
	BLT done
	MOVD $35, R8 // unlinkat(AT_FDCWD, argv[0], 0)
	MOVD $-100, R0
	MOVD 8(R19), R1
	MOVD $0, R2
	SVC $0
	CBNZ R0, done
	MOVD $35, R8 // unlinkat(AT_FDCWD, argv[1], AT_REMOVEDIR)
	MOVD $-100, R0
	MOVD 16(R19), R1
	MOVD $512, R2
	SVC $0
	CBNZ R0, done
	ADD $24, R19, R20
	MOVD $0, R23
next:
	MOVD (R20), R1
	CBZ R1, success
retry:
	MOVD $291, R8 // statx(AT_FDCWD, path, AT_NO_AUTOMOUNT, STATX_TYPE, buf)
	MOVD $-100, R0
	MOVD $2048, R2
	MOVD $1, R3
	MOVD RSP, R4
	SVC $0
	CMN $4, R0 // EINTR
	BEQ retry
	CBZ R0, complete
	CBNZ R23, done
	MOVD $1, R23
	B retry
complete:
	MOVHU 28(RSP), R5 // stx_mode must describe a directory
	AND $0xf000, R5
	CMP $0x4000, R5
	BNE done
	MOVD $0, R23
	ADD $8, R20
	B next
success:
	MOVD $0, R24
done:
	MOVD R24, R0
	MOVD $93, R8 // exit(status)
	SVC $0
