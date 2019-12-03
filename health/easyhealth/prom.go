package easyhealth

import (
	"github.com/feynman-go/workshop/health"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
	"strings"
)

type HealthProm struct {
	manager           *health.HealthManager
	logger            *zap.Logger
	compHealthDesc    *prometheus.Desc
	summaryHealthDesc *prometheus.Desc
}

func New(manager *health.HealthManager, serviceName string, logger *zap.Logger) *HealthProm {
	serviceName = strings.Replace(serviceName, "-", "_", -1)
	compHealthDesc := prometheus.NewDesc(serviceName + "_health_comp", "", []string{"module", "status", "level"}, prometheus.Labels{})
	summaryHealthDesc := prometheus.NewDesc(serviceName + "_health_bad_sum", "", []string{"level"}, prometheus.Labels{})

	return &HealthProm{
		manager:           manager,
		logger:            logger,
		compHealthDesc:    compHealthDesc,
		summaryHealthDesc: summaryHealthDesc,
	}
}

func (prom *HealthProm) Collect(ch chan<- prometheus.Metric) {
	report, err := prom.manager.GetReport()
	if err != nil {
		prom.logger.Error("get report", zap.Error(err))
		return
	}

	ch <- prometheus.MustNewConstMetric(prom.summaryHealthDesc, prometheus.CounterValue, float64(report.BadCritical), health.HealthLevelCritical.String())
	ch <- prometheus.MustNewConstMetric(prom.summaryHealthDesc, prometheus.CounterValue, float64(report.BadHeighLevel), health.HealthLevelHighPriority.String())
	ch <- prometheus.MustNewConstMetric(prom.summaryHealthDesc, prometheus.CounterValue, float64(report.BadLowLevel), health.HealthLevelLowPriority.String())


	list := make([]health.CheckInfo, 0, len(report.Expose) + len(report.External) + len(report.Internal))

	list = append(list, report.Expose...)
	list = append(list, report.External...)
	list = append(list, report.Internal...)

	for _, comp := range list {
		ss := comp.Record.Status
		var status = health.StatusUnknown
		if len(ss) > 0 {
			status = ss[0]
		}

		ch <- prometheus.MustNewConstMetric(
			prom.compHealthDesc,
			prometheus.CounterValue,
			float64(status),
			comp.Desc.Name,
			status.String(),
			comp.Desc.Level.String())
	}
}

func (prom *HealthProm) Describe(ch chan<- *prometheus.Desc) {
	ch <- prom.compHealthDesc
	ch <- prom.summaryHealthDesc
}

func (prom *HealthProm) MustRegister() {
	prometheus.MustRegister(prom)
}