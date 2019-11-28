package health

import (
	"context"
	"errors"
	"fmt"
	"go.uber.org/zap"
	"sync"
	"time"
)

const (
	HealthLevelCritical Level = 0
	HealthLevelHighPriority Level = 100
	HealthLevelLowPriority Level = 200

	TypeInternal Type = 0
	TypeExternal Type = 100
	TypeExpose   Type = 200

	StatusStopping StatusCode = -5
	StatusAbnormal StatusCode = -4
	StatusBadInterval StatusCode = -3
	StatusFatal StatusCode = -2
	StatusDown StatusCode = -1
	StatusUnknown StatusCode = 0
	StatusUp StatusCode = 1
	StatusOk StatusCode = 2
	StatusWarning StatusCode = 3

)


type Level int32

func (level Level) String() string {
	switch level {
	case HealthLevelCritical:
		return "critical"
	case HealthLevelHighPriority:
		return "high"
	case HealthLevelLowPriority:
		return "low"
	default:
		return "unknown"
	}
}

type Type int32

type StatusCode int32

type Status []StatusCode

func (ss Status) IsUp() bool {
	for _, s := range ss {
		if s >= StatusUp {
			return true
		}
	}
	return false
}

func (sc StatusCode) String() string {
	switch sc {
	case StatusStopping:
		return "stopping"
	case StatusAbnormal:
		return "abnormal"
	case StatusBadInterval:
		return "badInterval"
	case StatusFatal:
		return "fatal"
	case StatusDown:
		return "down"
	case StatusUp:
		return "up"
	case StatusOk:
		return "ok"
	case StatusWarning:
		return "warning"
	default:
		return "unknown"
	}
}

type CheckRecord struct {
	Status Status `json:"status"`
	CheckTime time.Time `json:"check_time"`
	CheckDuration time.Duration `json:"check_duration"`
	CheckDetail string `json:"check_detail"`
	SelfUpload bool `json:"self_upload"`
}

func (record CheckRecord) IsBadStatus() bool {
	for _, s := range record.Status {
		if s < 0 {
			return true
		}
	}
	return false
}

type CheckDesc struct {
	Name string `json:"name"`
	Type Type `json:"type"`
	Level Level `json:"level"`
}

type CheckInfo struct {
	Desc   CheckDesc
	Record CheckRecord
}

type Checker interface {
	Check(ctx context.Context, ) error
}

type CheckOption struct {
	Deep bool
}

type component struct {
	checker Checker
	desc    CheckDesc
	plan    CheckerPlan
	record  CheckRecord
}

type CheckerPlan struct {
	DeepPercent     float32
	DeepInterval    time.Duration
	ShallowInterval time.Duration
	DeepDuration    time.Duration
	ShallowDuration time.Duration
}

type HealthManager struct {
	critical         map[string]*component
	highPriority     map[string]*component
	lowPriority      map[string]*component
	rw               sync.RWMutex
	opt				 ManagerOption
	logger 			 *zap.Logger
}

type ManagerOption struct {
	DefaultPlan CheckerPlan
}

func NewManager(opt ManagerOption, logger *zap.Logger) *HealthManager {
	manager := &HealthManager{
		critical:         map[string]*component{},
		highPriority:     map[string]*component{},
		lowPriority:      map[string]*component{},
		rw:               sync.RWMutex{},
		opt: 			  opt,
		logger: logger,
	}
	manager.opt.DefaultPlan = DefaultCheckPlan
	manager.opt.DefaultPlan = manager.fullCheckPlan(opt.DefaultPlan)
	return manager
}

func (manager *HealthManager) UpdateCheckRecord(name string, record CheckRecord) error {
	var (
		m map[string]*component
	)

	manager.rw.Lock()
	defer manager.rw.Unlock()

	if _, ext := manager.critical[name]; ext {
		m = manager.critical
	}
	if _, ext := manager.highPriority[name]; ext {
		m = manager.highPriority
	}
	if _, ext := manager.lowPriority[name]; ext {
		m = manager.lowPriority
	}

	if m == nil {
		return fmt.Errorf("not has manager for name %v", name)
	}
	info := m[name]
	info.record = record
	m[name] = info
	return nil
}


func (manager *HealthManager) IsUp() bool {
	for _, comp := range manager.critical {
		if !comp.record.Status.IsUp() {
			return false
		}
	}
	return true
}

func (manager *HealthManager) UpdateChecker(desc CheckDesc, plan CheckerPlan, checker Checker) (*StatusReporter, error) {

	var (
		name = desc.Name
		from map[string]*component
		dest map[string]*component
		comp *component
	)

	manager.rw.Lock()
	defer manager.rw.Unlock()

	plan = manager.fullCheckPlan(plan)

	if _, ext := manager.critical[name]; ext {
		from = manager.critical
	}
	if _, ext := manager.highPriority[name]; ext {
		from = manager.highPriority
	}
	if _, ext := manager.lowPriority[name]; ext {
		from = manager.lowPriority
	}

	if from != nil {
		if comp = from[desc.Name]; comp.desc.Level != desc.Level {
			delete(from, name)
		}
	}

	switch desc.Level {
	case HealthLevelCritical:
		dest = manager.critical
	case HealthLevelHighPriority:
		dest = manager.highPriority
	case HealthLevelLowPriority:
		dest = manager.lowPriority
	default:
		return nil, errors.New("bad desc check level")
	}

	if comp == nil {
		comp = &component{
			record: CheckRecord{
				Status:        Status{StatusDown},
				CheckTime:     time.Now(),
			},
		}
	}

	comp.plan = plan
	comp.desc = desc
	comp.checker = checker

	dest[desc.Name] = comp
	return &StatusReporter{
		name: desc.Name,
		manager: manager,
	}, nil
}

func (manager *HealthManager) fullCheckPlan(plan CheckerPlan) CheckerPlan {
	if plan.DeepDuration == 0 {
		plan.DeepDuration = manager.opt.DefaultPlan.DeepDuration
	}
	if plan.DeepInterval == 0 {
		plan.DeepInterval = manager.opt.DefaultPlan.DeepInterval
	}
	if plan.ShallowInterval == 0 {
		plan.ShallowInterval = manager.opt.DefaultPlan.ShallowInterval
	}
	if plan.ShallowDuration == 0 {
		plan.ShallowDuration = manager.opt.DefaultPlan.ShallowDuration
	}
	if plan.DeepPercent == 0 {
		plan.DeepPercent = manager.opt.DefaultPlan.DeepPercent
	}

	return plan
}

func (manager *HealthManager) run(ctx context.Context) {

}

func (manager *HealthManager) GetReport() (Report, error) {
	manager.rw.RLock()
	defer manager.rw.RUnlock()

	var (
		rep Report
		components = []map[string]*component{
			manager.critical,
			manager.lowPriority,
			manager.highPriority,
		}
	)

	for _, cMap :=  range components {
		for _, comp := range cMap {
			manager.appendReport(comp.plan, CheckInfo{
				Desc:   comp.desc,
				Record: comp.record,
			}, &rep)
		}
	}

	return rep, nil
}


func (manager *HealthManager) appendReport(plan CheckerPlan, info CheckInfo, report *Report) {
	/*if plan.DeepInterval != 0 && time.Now().Sub(info.Record.CheckTime) > 2 * plan.DeepInterval {
		info.Record.Status = append(info.Record.Status, StatusBadInterval)
	}*/
	switch info.Desc.Type {
	case TypeInternal:
		report.Internal = append(report.Internal, info)
	case TypeExternal:
		report.External = append(report.External, info)
	case TypeExpose:
		report.Expose = append(report.Expose, info)
	}

	switch info.Desc.Level {
	case HealthLevelCritical:
		if info.Record.IsBadStatus() {
			report.BadCritical ++
		}
	case HealthLevelHighPriority:
		if info.Record.IsBadStatus() {
			report.BadHeighLevel++
		}
	case HealthLevelLowPriority:
		if info.Record.IsBadStatus() {
			report.BadLowLevel ++
		}
	}
}


var DefaultCheckPlan = CheckerPlan{
	DeepPercent:     float32(0.2),
	DeepInterval:    10 * time.Second,
	ShallowInterval: 10 * time.Second,
	DeepDuration:    0,
	ShallowDuration: 0,
}

type Report struct {
	BadCritical   int32
	BadHeighLevel int32
	BadLowLevel   int32
	Internal      []CheckInfo
	External      []CheckInfo
	Expose        []CheckInfo
}



func Default() *HealthManager {
	_rw.RLock()
	defer _rw.RUnlock()
	return _dManager
}


func ReplaceDefault(manager *HealthManager)  {
	_rw.Lock()
	defer _rw.Unlock()
	_dManager = manager
}


var _rw sync.RWMutex
var _dManager *HealthManager // default manager

func init() {
	_dManager = NewManager(ManagerOption{}, zap.L())
}

type StatusReporter struct {
	name string
	manager *HealthManager
}

func (reporter *StatusReporter) ReportStatus(detail string, status ...StatusCode) {
	err := reporter.manager.UpdateCheckRecord(reporter.name, CheckRecord{
		Status: status,
		SelfUpload: true,
		CheckTime: time.Now(),
		CheckDetail: detail,
	})
	reporter.manager.logger.Info("UpdateCheckRecord", zap.Error(err), zap.Any("status", status), zap.String("detail", detail))
}