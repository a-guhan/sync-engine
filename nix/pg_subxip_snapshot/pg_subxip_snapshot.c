/*-------------------------------------------------------------------------
 *
 * pg_subxip_snapshot.c
 *	  Expose the current snapshot as text, using subxip in recovery.
 *
 * On a primary, this mirrors pg_current_snapshot()::text.
 * During recovery, it formats a snapshot-like string using the current
 * snapshot's subxip[] entries filtered to the range [xmin, xmax).
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include <stdlib.h>

#include "access/transam.h"
#include "access/xlog.h"
#include "fmgr.h"
#include "utils/builtins.h"
#include "utils/snapmgr.h"

PG_MODULE_MAGIC;

PG_FUNCTION_INFO_V1(pg_current_subxip_snapshot_text);

static int	full_xid_qsort_cmp(const void *a, const void *b);
static uint32 collect_full_xids(FullTransactionId *dst, Snapshot cur,
								FullTransactionId next_fxid, bool in_recovery);
static text *snapshot_text_from_active_snapshot(void);

Datum
pg_current_subxip_snapshot_text(PG_FUNCTION_ARGS)
{
	PG_RETURN_TEXT_P(snapshot_text_from_active_snapshot());
}

static int
full_xid_qsort_cmp(const void *a, const void *b)
{
	FullTransactionId fa = *(const FullTransactionId *) a;
	FullTransactionId fb = *(const FullTransactionId *) b;

	if (FullTransactionIdPrecedes(fa, fb))
		return -1;
	if (FullTransactionIdFollows(fa, fb))
		return 1;
	return 0;
}

static uint32
collect_full_xids(FullTransactionId *dst, Snapshot cur,
				  FullTransactionId next_fxid, bool in_recovery)
{
	uint32		count = 0;

	if (!in_recovery)
	{
		uint32		i;

		for (i = 0; i < cur->xcnt; i++)
			dst[count++] =
				FullTransactionIdFromAllowableAt(next_fxid, cur->xip[i]);
	}
	else
	{
		int32		i;

		for (i = 0; i < cur->subxcnt; i++)
		{
			TransactionId xid = cur->subxip[i];

			if (TransactionIdPrecedes(xid, cur->xmin))
				continue;
			if (TransactionIdFollowsOrEquals(xid, cur->xmax))
				continue;

			dst[count++] = FullTransactionIdFromAllowableAt(next_fxid, xid);
		}
	}

	if (count > 1)
	{
		uint32		write_idx = 1;
		uint32		i;

		qsort(dst, count, sizeof(FullTransactionId), full_xid_qsort_cmp);

		for (i = 1; i < count; i++)
		{
			if (!FullTransactionIdEquals(dst[i], dst[write_idx - 1]))
				dst[write_idx++] = dst[i];
		}

		count = write_idx;
	}

	return count;
}

static text *
snapshot_text_from_active_snapshot(void)
{
	Snapshot	cur;
	FullTransactionId next_fxid;
	FullTransactionId *xids;
	FullTransactionId xmin_fxid;
	FullTransactionId xmax_fxid;
	StringInfoData str;
	uint32		nxid;
	uint32		i;
	bool		in_recovery;
	Size		max_entries;

	cur = GetActiveSnapshot();
	if (cur == NULL)
		elog(ERROR, "no active snapshot set");

	if (!TransactionIdIsValid(cur->xmin) || !TransactionIdIsValid(cur->xmax))
		elog(ERROR, "active snapshot does not have valid xmin/xmax");

	next_fxid = ReadNextFullTransactionId();
	xmin_fxid = FullTransactionIdFromAllowableAt(next_fxid, cur->xmin);
	xmax_fxid = FullTransactionIdFromAllowableAt(next_fxid, cur->xmax);
	in_recovery = RecoveryInProgress();

	max_entries = in_recovery ? Max(cur->subxcnt, 0) : cur->xcnt;
	xids = max_entries > 0 ? palloc(sizeof(FullTransactionId) * max_entries) : NULL;
	nxid = collect_full_xids(xids, cur, next_fxid, in_recovery);

	initStringInfo(&str);
	appendStringInfo(&str, UINT64_FORMAT ":",
					 U64FromFullTransactionId(xmin_fxid));
	appendStringInfo(&str, UINT64_FORMAT ":",
					 U64FromFullTransactionId(xmax_fxid));

	for (i = 0; i < nxid; i++)
	{
		if (i > 0)
			appendStringInfoChar(&str, ',');
		appendStringInfo(&str, UINT64_FORMAT,
						 U64FromFullTransactionId(xids[i]));
	}

	return cstring_to_text(str.data);
}
