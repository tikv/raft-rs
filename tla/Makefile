TLA2TOOLS_JAR_URL = https://github.com/tlaplus/tlaplus/releases/download/v1.8.0/tla2tools.jar
COMMUNITYMODULES_DEPS_JAR_URL = https://github.com/tlaplus/communityModules/releases/latest/download/CommunityModules-deps.jar
TLA ?= consensus/WrapperUdp.tla

.PHONY: all clean download run

download: tla2tools.jar CommunityModules-deps.jar

run: download
	exec java -Dtlc2.tool.fp.FPSet.impl=tlc2.tool.fp.OffHeapDiskFPSet -XX:+UseParallelGC -cp tla2tools.jar:CommunityModules-deps.jar tlc2.TLC $(TLA) -workers auto -deadlock -cleanup

tla2tools.jar:
	wget -O tla2tools.jar $(TLA2TOOLS_JAR_URL)
	
CommunityModules-deps.jar:
	wget -O CommunityModules-deps.jar $(COMMUNITYMODULES_DEPS_JAR_URL)

clean:
	rm -rf states*