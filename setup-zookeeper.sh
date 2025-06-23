
#!/bin/bash

ZOOKEEPER_VERSION="3.8.4"
ZOOKEEPER_DIR="apache-zookeeper-$ZOOKEEPER_VERSION-bin"
ZOOKEEPER_TAR="$ZOOKEEPER_DIR.tar.gz"
DOWNLOAD_URL="https://downloads.apache.org/zookeeper/zookeeper-$ZOOKEEPER_VERSION/$ZOOKEEPER_TAR"

# Check if ZooKeeper is already installed
if [ -d "$ZOOKEEPER_DIR" ]; then
  echo "✅ ZooKeeper already installed in $ZOOKEEPER_DIR"
else
  echo "⬇️ Downloading ZooKeeper $ZOOKEEPER_VERSION..."
  curl -O $DOWNLOAD_URL
  if [ ! -f "$ZOOKEEPER_TAR" ]; then
    echo "❌ Download failed – check the ZooKeeper version or network access."
    exit 1
  fi

  tar -xzf $ZOOKEEPER_TAR || { echo "❌ Failed to extract tarball"; exit 1; }

  echo "🛠️ Creating ZooKeeper config..."
  cd $ZOOKEEPER_DIR || { echo "❌ Could not enter directory $ZOOKEEPER_DIR"; exit 1; }
  mkdir -p data
  cat <<EOF > conf/zoo.cfg
tickTime=2000
dataDir=./data
clientPort=2181
initLimit=5
syncLimit=2
EOF
  cd ..
fi

# Start ZooKeeper
echo "🚀 Starting ZooKeeper..."
cd $ZOOKEEPER_DIR || { echo "❌ Could not enter directory $ZOOKEEPER_DIR"; exit 1; }
./bin/zkServer.sh start conf/zoo.cfg
