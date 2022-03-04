package rotor_test

import (
	"context"
	"errors"
	"math/rand"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb/expression"
	"github.com/lixw1994/rotor"
)

const (
	pKPrefix = "Test#"
	sk       = "Test"
)

type TestSchema struct {
	rotor.BaseSchema

	TestV string
}

func newTestSchema(id string, v string) *TestSchema {
	nowTS := time.Now().Unix()
	base := rotor.BaseSchema{
		PK:         pKPrefix + id,
		SK:         sk,
		Version:    randomID(8),
		CreateTime: nowTS,
		UpdateTime: nowTS,
	}
	schema := TestSchema{
		BaseSchema: base,
		TestV:      v,
	}
	return &schema
}

func randomID(n int) string {
	charset := "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	b := make([]byte, n)
	for i := range b {
		n := r.Intn(len(charset))
		b[i] = charset[n]
	}
	return string(b)
}

func TestOp(t *testing.T) {
	id := ""
	secret := ""
	region := ""
	endpoint := ""
	tableName := ""
	sess, err := session.NewSession(&aws.Config{
		Credentials: credentials.NewStaticCredentials(id, secret, ""),
		Region:      aws.String(region),
		Endpoint:    aws.String(endpoint),
		MaxRetries:  aws.Int(3),
	})
	if err != nil {
		t.Fatal(err)
		return
	}
	rs := rotor.New(sess, tableName)
	t.Run("Put", func(t *testing.T) {
		t.Run("Put-OK", func(t *testing.T) {
			err := rs.Put(context.TODO(), newTestSchema("id1", "v1"))
			if err != nil {
				t.Errorf("Put失败: %v", err)
				return
			}
		})
		t.Run("Put-IfNotExist", func(t *testing.T) {
			err := rs.PutIfNotExist(context.TODO(), newTestSchema("id1", "v1"))
			if err != nil {
				if !errors.Is(err, rotor.ErrConditionalCheck) {
					t.Errorf("PutIfNotExist失败: %v", err)
					return
				}
				return
			}
			t.Error("PutIfNotExist不应该成功")
			return
		})
		t.Run("Put-Condition", func(t *testing.T) {
			// 只有存在才会写入
			err := rs.Put(context.TODO(), newTestSchema("id1", "v1"),
				rotor.PutCondition(rotor.ConditionItemExist()))
			if err != nil {
				t.Errorf("PutIfNotExist失败: %v", err)
				return
			}
		})
	})
	t.Run("Get", func(t *testing.T) {
		t.Run("Get-OK", func(t *testing.T) {
			var out TestSchema
			err := rs.Get(context.TODO(), rotor.PrimaryKey(pKPrefix+"id1", sk), &out)
			if err != nil {
				t.Errorf("Get失败: %v", err)
				return
			}
			t.Logf("Get: %v", out)
		})
		t.Run("Get-NotExist", func(t *testing.T) {
			var out TestSchema
			err := rs.Get(context.TODO(), rotor.PrimaryKey(pKPrefix+randomID(8), sk), &out)
			if err != nil {
				if !errors.Is(err, rotor.ErrItemNotFound) {
					t.Errorf("Get-NotExist失败: %v", err)
					return
				}
				return
			}
			t.Error("Get-NotExist不应该成功")
			return
		})
		t.Run("Get-Projection", func(t *testing.T) {
			var out TestSchema
			err := rs.Get(context.TODO(), rotor.PrimaryKey(pKPrefix+"id1", sk), &out,
				rotor.GetProjection(expression.NamesList(expression.Name("TestV"))))
			if err != nil {
				t.Errorf("Get失败: %v", err)
				return
			}
			t.Logf("Get: %v", out)
		})
	})
	t.Run("Update", func(t *testing.T) {
		t.Run("Update-OK", func(t *testing.T) {
			var out TestSchema
			update := expression.Set(expression.Name("TestV"), expression.Value("v2"))
			err := rs.UpdateOut(context.TODO(), rotor.PrimaryKey(pKPrefix+"id1", sk), update, &out,
				rotor.UpdateCondition(rotor.ConditionItemExist()))
			if err != nil {
				t.Errorf("Update失败: %v", err)
				return
			}
			if out.TestV != "v2" {
				t.Error("Update失败: 不是预期的值")
				return
			}
			t.Logf("Update Out: %v", out)
		})
		t.Run("Update-Batch", func(t *testing.T) {
			update := expression.Set(expression.Name("TestV"), expression.Value("v3"))
			err := rs.UpdateBatch(context.TODO(), []rotor.PrimaryKeyType{rotor.PrimaryKey(pKPrefix+"id1", sk)}, update)
			if err != nil {
				t.Errorf("Update Batch失败: %v", err)
				return
			}
			var outs []TestSchema
			err = rs.GetBatch(context.TODO(), []rotor.PrimaryKeyType{rotor.PrimaryKey(pKPrefix+"id1", sk)}, &outs,
				rotor.GetProjection(expression.NamesList(expression.Name("TestV"))))
			if err != nil {
				t.Errorf("GetBatch失败: %v", err)
				return
			}
			if len(outs) != 1 {
				t.Error("Update失败: 不是预期的长度")
				return
			}
			if outs[0].TestV != "v3" {
				t.Error("Update失败: 不是预期的值")
				return
			}
			t.Logf("UpdateBatch One: %v", outs[0])
		})
	})
	t.Run("Delete", func(t *testing.T) {
		t.Run("Delete-OK", func(t *testing.T) {
			// 重复删除不会报错
			err := rs.Delete(context.TODO(), rotor.PrimaryKey(pKPrefix+"idxx", sk))
			if err != nil {
				t.Errorf("Delete失败: %v", err)
				return
			}
			err = rs.Delete(context.TODO(), rotor.PrimaryKey(pKPrefix+"idxx", sk))
			if err != nil {
				t.Errorf("Delete失败: %v", err)
				return
			}
		})
		t.Run("Delete-Condition", func(t *testing.T) {
			err := rs.Delete(context.TODO(), rotor.PrimaryKey(pKPrefix+"id1", sk),
				rotor.DeleteCondition(expression.Equal(expression.Name("TestV"), expression.Value("v_not_exist"))),
			)
			if err != nil {
				if !errors.Is(err, rotor.ErrConditionalCheck) {
					t.Errorf("Delete失败: %v", err)
					return
				}
				return
			}
			t.Error("Delete不应该成功")
		})
		t.Run("Delete-Out", func(t *testing.T) {
			var out TestSchema
			err := rs.DeleteOut(context.TODO(), rotor.PrimaryKey(pKPrefix+"id1", sk), &out,
				rotor.DeleteCondition(expression.Equal(expression.Name("TestV"), expression.Value("v3"))),
			)
			if err != nil {
				t.Errorf("Delete失败: %v", err)
			}
			t.Logf("Delete: %v", out)
		})
	})
}
