package model

import (
	"context"

	"cloud.google.com/go/bigtable"
)

func GetAdminClient(ctx context.Context, project, instance string) (*bigtable.AdminClient, error) {
	adminClient, err := bigtable.NewAdminClient(ctx, project, instance)
	if err != nil {
		return nil, err
	}

	return adminClient, nil
}
