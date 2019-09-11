package mongo

import (
	"context"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"testing"
	"time"
)

func TestTaskScheduler(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	client, err := mongo.Connect(context.Background(), options.Client().SetHosts([]string{"localhost:27017"}).SetDirect(true))
	require.NoError(t, err)

	defer client.Disconnect(ctx)


}
