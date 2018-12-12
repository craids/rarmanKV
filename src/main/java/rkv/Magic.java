
package rkv;

interface Magic {

	short FILE_HEADER = 0x1350;

	short PAGE_MAGIC = 0x1351;

	short FREE_PAGE = 0;
	short USED_PAGE = 1;
	short TRANSLATION_PAGE = 2;
	short FREELOGIDS_PAGE = 3;
	short FREEPHYSIDS_PAGE = 4;
	short FREEPHYSIDS_ROOT_PAGE = 5;

	short NLISTS = 6;

	short LOGFILE_HEADER = 0x1360;

	short SZ_BYTE = 1;

	short SZ_SHORT = 2;

	short SZ_INT = 4;

	short SZ_LONG = 8;

	short SZ_SIX_BYTE_LONG = 6;

	short FILE_HEADER_O_MAGIC = 0; // short magic
	short FILE_HEADER_O_LISTS = Magic.SZ_SHORT; // long[2*NLISTS]
	int FILE_HEADER_O_ROOTS = FILE_HEADER_O_LISTS + (Magic.NLISTS * 2 * Magic.SZ_LONG);

	int FILE_HEADER_NROOTS = 16;

	short PAGE_HEADER_O_MAGIC = 0; // short magic
	short PAGE_HEADER_O_NEXT = Magic.SZ_SHORT;
	short PAGE_HEADER_O_PREV = PAGE_HEADER_O_NEXT + Magic.SZ_SIX_BYTE_LONG;
	short PAGE_HEADER_SIZE = PAGE_HEADER_O_PREV + Magic.SZ_SIX_BYTE_LONG;

	short PhysicalRowId_O_LOCATION = 0; // long page
//    short PhysicalRowId_O_OFFSET = Magic.SZ_SIX_BYTE_LONG; // short offset
	int PhysicalRowId_SIZE = Magic.SZ_SIX_BYTE_LONG;

	short DATA_PAGE_O_FIRST = PAGE_HEADER_SIZE; // short firstrowid
	short DATA_PAGE_O_DATA = (short) (DATA_PAGE_O_FIRST + Magic.SZ_SHORT);
	short DATA_PER_PAGE = (short) (Storage.PAGE_SIZE - DATA_PAGE_O_DATA);

}
