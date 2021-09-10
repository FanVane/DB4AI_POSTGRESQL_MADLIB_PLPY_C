MODULES = opengaussOPs # 要构建的共享库的列表

PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)