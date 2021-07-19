package main_test

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"testing"
)

func TestDidiNgproxy(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "TestDidiNgproxy Suite")
}
