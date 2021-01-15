package main

import (
	"context"

	"github.com/sirupsen/logrus"
	"github.com/tsuna/gohbase"
	"github.com/tsuna/gohbase/hrpc"
)

func main() {
	auth := gohbase.Auth("KERBEROS")
	user := gohbase.EffectiveUser("hbase-kerberos@GLAB.COM")
	root := gohbase.ZookeeperRoot("/hbase-secure")
	options := []gohbase.Option{auth, user, root}

	client := gohbase.NewClient("mix1-test-glab-com", options...)

	logrus.SetLevel(logrus.DebugLevel)
	req, err := hrpc.NewGetStr(context.Background(), "t1", "aaaa")
	if err != nil {
		panic(err)
	}

	logrus.Infof("Getting...\n")
	getRsp, err := client.Get(req)
	if err != nil {
		panic(err)
	}

	logrus.Infof("%v\n", getRsp)
}

func admin(){
	auth := gohbase.Auth("KERBEROS")
	user := gohbase.EffectiveUser("hbase-kerberos@GLAB.COM")
	root := gohbase.ZookeeperRoot("/hbase-secure")
	options := []gohbase.Option{auth, user, root}

	client := gohbase.NewAdminClient("mix1-test-glab-com", options...)

	_, err := hrpc.NewListTableNames(context.Background())
	if err != nil {
		panic(err)
	}

	logrus.Infof("Listing Table Names...\n")
	getRsp, err := client.ClusterStatus()
	if err != nil {
		panic(err)
	}

	logrus.Infof("%v\n", getRsp)
}
