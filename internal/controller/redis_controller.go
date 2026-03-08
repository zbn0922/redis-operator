/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package controller

import (
	"bytes"
	"context"
	"reflect"
	systemRuntime "runtime"
	"sort"
	"strings"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	autoscalingv2 "k8s.io/api/autoscaling/v2"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/remotecommand"
	"k8s.io/client-go/util/retry"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	zbn0922v1 "github.com/zbn0922/redis-operator/api/v1"
)

// RedisReconciler reconciles a Redis object
type RedisReconciler struct {
	client.Client
	Scheme    *runtime.Scheme
	Config    *rest.Config
	Clientset *kubernetes.Clientset
}

const RedisFinalizer = "redis.zbn0922.github.com/finalizer"

// +kubebuilder:rbac:groups=zbn0922.github.com,resources=redis,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=zbn0922.github.com,resources=redis/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=zbn0922.github.com,resources=redis/finalizers,verbs=update

// Reconcile is part of the main kubernetes reconciliation loop which aims to
// move the current state of the cluster closer to the desired state.
// TODO(user): Modify the Reconcile function to compare the state specified by
// the Redis object against the actual cluster state, and then
// perform operations to make the cluster state reflect the state specified by
// the user.
//
// For more details, check Reconcile and its Result here:
// - https://pkg.go.dev/sigs.k8s.io/controller-runtime@v0.23.1/pkg/reconcile
func (r *RedisReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := logf.FromContext(ctx)

	// 1、从cache获取资源
	var redis zbn0922v1.Redis
	if err := r.Get(ctx, req.NamespacedName, &redis); err != nil {
		// 如果获取不到资源应该已经删除
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	// 1、 清理资源
	if !redis.DeletionTimestamp.IsZero() {
		if controllerutil.ContainsFinalizer(&redis, RedisFinalizer) {

			// TODO: 删除时清理资源，优雅退出，备份、日志、通知之类的,
			if err := r.cleanupRedis(ctx, &redis); err != nil {
				return ctrl.Result{}, err
			}

			controllerutil.RemoveFinalizer(&redis, RedisFinalizer)
			if err := r.Update(ctx, &redis); err != nil {
				return ctrl.Result{}, err
			}
			return ctrl.Result{}, nil
		}
	}
	// 2、添加finalizer
	if redis.DeletionTimestamp.IsZero() {
		if !controllerutil.ContainsFinalizer(&redis, RedisFinalizer) {
			controllerutil.AddFinalizer(&redis, RedisFinalizer)

			if err := r.Update(ctx, &redis); err != nil {
				return ctrl.Result{}, err
			}
			return ctrl.Result{}, nil
		}
	}

	// 获取当前 goroutine ID
	var buf [64]byte
	n := systemRuntime.Stack(buf[:], false)
	goroutineID := strings.Fields(string(buf[:n]))[1]
	//log.GetSink().Info()
	log.Info(
		"Redis spec info",
		"name", req.NamespacedName,
		"replicas", redis.Spec.Replicas, // int32 类型直接传，zap 会自动格式化
		"image", redis.Spec.Image,
		"storage", redis.Spec.Storage,
		"redis_resourceVersion", redis.ResourceVersion,
		"redis_generation", redis.Generation,
		"time", time.Now().Format("2006-01-02 15:04:05.000"),
		"goroutineID", goroutineID,
	)
	// 3、创建hardless service
	if res, err := r.reconcileService(ctx, &redis); err != nil || res != nil {
		return DefaultIfEmpty(res), err
	}
	// 4、查看是否存在statefulset,不存在创建
	if res, err := r.reconcileStatefulSet(ctx, &redis); err != nil || res != nil {
		return DefaultIfEmpty(res), err
	}
	//var sts appsv1.StatefulSet

	// 5、自动扩缩容
	if res, err := r.reconcileHPA(ctx, &redis); err != nil || res != nil {
		return DefaultIfEmpty(res), err
	}
	// 6、 status
	if res, err := r.updateStatusWithRetry(ctx, &redis); err != nil || res != nil {
		return DefaultIfEmpty(res), err
	}

	//return ctrl.Result{}, fmt.Errorf("test error") // 验证是否是指数退避，正确的这个位置应该是 return ctrl.Result{}, nil
	return ctrl.Result{}, nil
}

// DefaultIfEmpty 如果 res 为 nil，返回默认的 empty Result，否则返回 res
func DefaultIfEmpty(res *ctrl.Result) ctrl.Result {
	if res != nil {
		return *res
	}
	return ctrl.Result{}
}

func (r *RedisReconciler) cleanupRedis(ctx context.Context, redis *zbn0922v1.Redis) error {

	// 示例：删除 StatefulSet
	//var sts appsv1.StatefulSet

	//err := r.Get(ctx, types.NamespacedName{
	//	Name:      redis.Name,
	//	Namespace: redis.Namespace,
	//}, &sts)

	//if err == nil {
	//	return r.Delete(ctx, &sts)
	//}
	log := logf.FromContext(ctx)

	log.Info("start cleanupRedis")
	// 这个时候，kubectl delete 会一直卡着，等待响应直到返回后
	//time.Sleep(time.Minute)
	log.Info("end cleanupRedis")

	return nil
}

func (r *RedisReconciler) reconcileService(ctx context.Context, redis *zbn0922v1.Redis) (*ctrl.Result, error) {
	name := redis.Name
	var svc corev1.Service
	key := types.NamespacedName{
		Name:      name,
		Namespace: redis.Namespace,
	}
	err := r.Get(ctx, key, &svc)
	if err != nil && apierrors.IsNotFound(err) {
		// 创建service
		newSvc := corev1.Service{
			ObjectMeta: metav1.ObjectMeta{
				Name:      redis.Name,
				Namespace: redis.Namespace,
			},
			Spec: corev1.ServiceSpec{
				ClusterIP: "None",
				Selector: map[string]string{
					"app": redis.Name,
				},
				Ports: []corev1.ServicePort{
					{
						Port: 6379,
					},
				},
			},
		}
		// 设置引用
		if err = ctrl.SetControllerReference(redis, &newSvc, r.Scheme); err != nil {
			return nil, err
		}
		// 创建
		if err = r.Create(ctx, &newSvc); err != nil {
			return nil, err
		}
		// 创建了service后，是异步更新资源，所以需要再次调用下，看下是否创建成功了
		return &ctrl.Result{}, nil
	} else if err != nil {
		return nil, err
	}

	return nil, nil
}

func (r *RedisReconciler) reconcileStatefulSet(ctx context.Context, redis *zbn0922v1.Redis) (*ctrl.Result, error) {
	log := logf.FromContext(ctx)
	stsName := redis.Name
	key := types.NamespacedName{
		Namespace: redis.Namespace,
		Name:      stsName,
	}
	var sts appsv1.StatefulSet
	err := r.Get(ctx, key, &sts)
	if err != nil && apierrors.IsNotFound(err) {
		// create
		newSts := appsv1.StatefulSet{
			ObjectMeta: metav1.ObjectMeta{
				Name:      stsName,
				Namespace: redis.Namespace,
			},
			Spec: appsv1.StatefulSetSpec{
				ServiceName: stsName,
				Replicas:    &redis.Spec.Replicas,
				Selector: &metav1.LabelSelector{
					MatchLabels: map[string]string{"app": stsName},
				},
				PersistentVolumeClaimRetentionPolicy: &appsv1.StatefulSetPersistentVolumeClaimRetentionPolicy{
					WhenDeleted: appsv1.DeletePersistentVolumeClaimRetentionPolicyType, // 删StatefulSet时删PVC
					WhenScaled:  appsv1.DeletePersistentVolumeClaimRetentionPolicyType, // 缩容删Pod时删PVC（比如replicas从1→0）
				},
				UpdateStrategy: appsv1.StatefulSetUpdateStrategy{
					Type: appsv1.RollingUpdateStatefulSetStrategyType,
					// 可选：配置滚动更新的参数（比如最多不可用1个Pod）
					RollingUpdate: &appsv1.RollingUpdateStatefulSetStrategy{
						MaxUnavailable: &intstr.IntOrString{
							Type:   intstr.Int,
							IntVal: 1, // 滚动更新时最多1个Pod不可用
						},
					},
				},
				Template: corev1.PodTemplateSpec{
					ObjectMeta: metav1.ObjectMeta{
						Labels: map[string]string{
							"app": stsName,
						},
					},
					Spec: corev1.PodSpec{
						Containers: []corev1.Container{
							{
								Name:  "redis",
								Image: redis.Spec.Image,
								Ports: []corev1.ContainerPort{
									{
										ContainerPort: 6379,
									},
								},
								VolumeMounts: []corev1.VolumeMount{
									{
										Name:      "data",
										MountPath: "/data",
									},
								},
								// 核心：添加 Readiness Probe
								ReadinessProbe: &corev1.Probe{
									ProbeHandler: corev1.ProbeHandler{
										Exec: &corev1.ExecAction{
											Command: []string{"redis-cli", "ping"}, // 验证 Redis 服务可用
										},
									},
									InitialDelaySeconds: 5, // 启动后5秒开始检测
									PeriodSeconds:       3, // 每3秒检测一次
									TimeoutSeconds:      1, // 检测超时1秒
									SuccessThreshold:    1, // 1次成功即认为就绪
									FailureThreshold:    3, // 3次失败则认为未就绪
								},
								// 可选：添加 Liveness Probe（检测 Redis 进程是否存活）
								LivenessProbe: &corev1.Probe{
									ProbeHandler: corev1.ProbeHandler{
										Exec: &corev1.ExecAction{
											Command: []string{"redis-cli", "ping"},
										},
									},
									InitialDelaySeconds: 10,
									PeriodSeconds:       5,
									TimeoutSeconds:      2,
									FailureThreshold:    3,
								},
								Resources: corev1.ResourceRequirements{
									Requests: corev1.ResourceList{
										corev1.ResourceCPU:    resource.MustParse("250m"),  // 请求 1 核
										corev1.ResourceMemory: resource.MustParse("512Mi"), // 请求 1Gi 内存
									},
									Limits: corev1.ResourceList{
										corev1.ResourceCPU:    resource.MustParse("500m"), // 限制 2 核
										corev1.ResourceMemory: resource.MustParse("1Gi"),  // 限制 2Gi 内存
									},
								},
							},
						},
					},
				},
				VolumeClaimTemplates: []corev1.PersistentVolumeClaim{
					{
						ObjectMeta: metav1.ObjectMeta{
							Name: "data",
						},
						Spec: corev1.PersistentVolumeClaimSpec{
							AccessModes: []corev1.PersistentVolumeAccessMode{
								corev1.ReadWriteOnce,
							},
							Resources: corev1.VolumeResourceRequirements{
								Requests: corev1.ResourceList{
									corev1.ResourceStorage: resource.MustParse(
										redis.Spec.Storage,
									),
								},
							},
						},
					},
				},
			},
		}
		// 添加资源引用, 主要的作用是在删除cr的时候同步删除cr创建的其他资源 gc watch delete
		if err = ctrl.SetControllerReference(redis, &newSts, r.Scheme); err != nil {
			return nil, err
		}
		if err = r.Create(ctx, &newSts); err != nil {
			return nil, err
		}
		log.Info("statefulset create success")
		return &ctrl.Result{}, nil
	}

	// 更新
	err = retry.RetryOnConflict(retry.DefaultRetry, func() error {
		var sts appsv1.StatefulSet
		if err := r.Get(ctx, key, &sts); err != nil {
			if apierrors.IsNotFound(err) {
				return nil
			}
			return err
		}
		specNew := sts.Spec.DeepCopy()
		if redis.Spec.AutoScale == nil {
			if specNew.Replicas == nil || *specNew.Replicas != redis.Spec.Replicas {
				specNew.Replicas = &redis.Spec.Replicas
			}
		}
		if specNew.Template.Spec.Containers[0].Image != redis.Spec.Image {
			specNew.Template.Spec.Containers[0].Image = redis.Spec.Image
		}
		if !reflect.DeepEqual(sts.Spec, *specNew) {
			sts.Spec = *specNew
			if err = r.Update(ctx, &sts); err != nil {
				return err
			}
			log.Info("statefulset update success")
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	// 获取pod list（得等待完成后在进行，所以这个位置获取不到的话，直接）
	var pods corev1.PodList
	err = r.List(
		ctx,
		&pods,
		client.InNamespace(redis.Namespace),
		client.MatchingLabels{
			"app": redis.Name,
		},
	)
	if err != nil {
		return nil, err
	}

	// 找到已经ready的pod
	var readyPods []corev1.Pod
	for _, item := range pods.Items {
		if isPodReady(item) {
			readyPods = append(readyPods, item)
		}
	}
	// 对readyPods 进行排序，因为这个顺序是由apiserver决定的，不一定是排序好的
	sort.Slice(readyPods, func(i, j int) bool {
		return readyPods[i].Name < readyPods[j].Name
	})
	//var specNum int32
	//if redis.Spec.AutoScale == nil {
	//	specNum = redis.Spec.Replicas
	//} else {
	//	specNum = *sts.Spec.Replicas
	//}
	//if len(readyPods) != int(specNum) { // 如果没有全部准备好，等待5秒在进行重试
	//	return &ctrl.Result{RequeueAfter: 5 * time.Second}, nil
	//}
	if len(readyPods) < 1 {
		return nil, nil
	}
	var master corev1.Pod
	if redis.Status.Master == "" {
		master = readyPods[0]
	} else {
		masterPod := electNewMaster(readyPods, redis.Status.Master)
		if masterPod == nil { // 如果没有找到新的pod那就5秒之后重试下
			return &ctrl.Result{RequeueAfter: 5 * time.Second}, nil
		}
		master = *masterPod
	}
	// 新的master先no one
	cmdNoOne := []string{
		"redis-cli",
		"--raw",
		"replicaof",
		"no",
		"one",
	}
	stdout, stderr, err := r.execRedisCommand(ctx, master, cmdNoOne)
	log.Info("statefulset get pod",
		"replica", master.Name,
		"ip", master.Status.PodIP,
		"stdout", stdout,
		"stderr", stderr,
		"err", err,
	)
	if err != nil || stderr != "" || strings.HasPrefix(stdout, "ERR") { // 如果出错5秒之后在重试
		return &ctrl.Result{RequeueAfter: 5 * time.Second}, nil
	}

	// Use Service DNS name instead of PodIP
	masterDNS := master.Name + "." + redis.Name + "." + redis.Namespace + ".svc.cluster.local"
	masterPort := "6379"
	log.Info("statefulset get pod", "master", master.Name, "dns", masterDNS)
	// pod已经全部准备好了，现在要配置主从了
	for _, pod := range readyPods {
		if pod.GetObjectMeta().GetName() == master.GetObjectMeta().GetName() {
			continue
		}
		// 判断是否已经执行过了
		cmdInfo := []string{
			"redis-cli",
			"--raw",
			"INFO",
			"replication",
		}
		stdout, stderr, err := r.execRedisCommand(ctx, pod, cmdInfo)
		log.Info("statefulset get pod",
			"replica", pod.Name,
			"ip", pod.Status.PodIP,
			"stdout", stdout,
			"stderr", stderr,
			"err", err,
		)
		if err != nil || stderr != "" || strings.HasPrefix(stdout, "ERR") || isAlreadyReplicaOf(stdout, masterDNS, masterPort) {
			continue
		}

		cmd := []string{
			"redis-cli",
			"--raw",
			"replicaof",
			masterDNS,
			"6379",
		}

		stdoutReplica, stderrReplica, err := r.execRedisCommand(ctx, pod, cmd)
		log.Info("statefulset get pod",
			"replica", pod.Name,
			"ip", pod.Status.PodIP,
			"stdout", stdoutReplica,
			"stderr", stderrReplica,
			"err", err,
		)

	}
	return nil, nil
}

func electNewMaster(pods []corev1.Pod, oldMaster string) *corev1.Pod {
	// 优先复用旧主且就绪的 Pod
	for i := range pods {
		if pods[i].Name == oldMaster && isPodReady(pods[i]) {
			return &pods[i]
		}
	}
	// 否则返回第一个就绪的非旧主 Pod
	for i := range pods {
		if pods[i].Name != oldMaster && isPodReady(pods[i]) {
			return &pods[i]
		}
	}
	return nil
}

// 在你的 reconciler 或 exec 函数附近加一个 helper 函数
func isAlreadyReplicaOf(infoOutput string, expectedMasterIP string, expectedMasterPort string) bool {
	lines := strings.Split(infoOutput, "\n")

	var role, masterHost, masterPort string

	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue // 跳过空行和 section 头
		}

		parts := strings.SplitN(line, ":", 2)
		if len(parts) != 2 {
			continue
		}

		key := strings.TrimSpace(parts[0])
		value := strings.TrimSpace(parts[1])

		switch key {
		case "role":
			role = value
		case "master_host":
			masterHost = value
		case "master_port":
			masterPort = value
			// 可以根据需要再加其他字段，比如 master_link_status == "up"
		}
	}

	// 判断条件：必须是 slave，且 master 的 IP 和端口完全匹配
	if role == "slave" &&
		masterHost == expectedMasterIP &&
		masterPort == expectedMasterPort {

		// 可选：再加更严格的检查
		// if masterLinkStatus != "up" { return false }

		return true
	}

	return false
}

func (r *RedisReconciler) findMater(ctx context.Context, pods corev1.PodList) corev1.Pod {
	// 找到master并返回
	var master corev1.Pod
	for _, pod := range pods.Items {
		if isPodReady(pod) {
			// 判断是否已经执行过了
			cmdInfo := []string{
				"redis-cli",
				"--raw",
				"INFO",
				"replication",
			}
			stdout, stderr, err := r.execRedisCommand(ctx, pod, cmdInfo)
			//if err != nil || stderr != "" || strings.HasPrefix(stdout, "ERR") || isAlreadyReplicaOf(stdout, masterIp, masterPort) {
			if err != nil || stderr != "" || strings.HasPrefix(stdout, "ERR") {
				continue
			}
			// 解析 INFO replication 输出，判断当前 Pod 是否为 master
			if strings.Contains(stdout, "role:master") {
				master = pod
				break
			}
		}
	}

	return master
}

func isPodReady(pod corev1.Pod) bool {
	for _, cond := range pod.Status.Conditions {
		if cond.Type == corev1.PodReady &&
			cond.Status == corev1.ConditionTrue {
			return true
		}
	}
	return false
}

func (r *RedisReconciler) reconcileHPA(ctx context.Context, redis *zbn0922v1.Redis) (*ctrl.Result, error) {
	log := logf.FromContext(ctx)
	if redis.Spec.AutoScale == nil {
		return nil, nil
	}

	name := redis.Name

	var hpa autoscalingv2.HorizontalPodAutoscaler

	key := types.NamespacedName{
		Name:      name,
		Namespace: redis.Namespace,
	}

	err := r.Get(ctx, key, &hpa)
	if err != nil && apierrors.IsNotFound(err) {

		newHpa := autoscalingv2.HorizontalPodAutoscaler{
			ObjectMeta: metav1.ObjectMeta{
				Name:      name,
				Namespace: redis.Namespace,
			},
			Spec: autoscalingv2.HorizontalPodAutoscalerSpec{

				ScaleTargetRef: autoscalingv2.CrossVersionObjectReference{
					APIVersion: "apps/v1",
					Kind:       "StatefulSet",
					Name:       name,
				},

				MinReplicas: &redis.Spec.AutoScale.MinReplicas,
				MaxReplicas: redis.Spec.AutoScale.MaxReplicas,

				Metrics: []autoscalingv2.MetricSpec{
					{
						Type: autoscalingv2.ResourceMetricSourceType,
						Resource: &autoscalingv2.ResourceMetricSource{
							Name: corev1.ResourceCPU,
							Target: autoscalingv2.MetricTarget{
								Type:               autoscalingv2.UtilizationMetricType,
								AverageUtilization: &redis.Spec.AutoScale.CPU,
							},
						},
					},
				},
			},
		}

		if err := ctrl.SetControllerReference(redis, &newHpa, r.Scheme); err != nil {
			return nil, err
		}

		if err := r.Create(ctx, &newHpa); err != nil {
			return nil, err
		}
		return &ctrl.Result{}, nil
	}

	// 更新
	err = retry.RetryOnConflict(retry.DefaultRetry, func() error {
		var hpa autoscalingv2.HorizontalPodAutoscaler
		if err := r.Get(ctx, key, &hpa); err != nil {
			return err
		}
		specNew := hpa.Spec.DeepCopy()

		if redis.Spec.AutoScale.MaxReplicas != specNew.MaxReplicas {
			specNew.MaxReplicas = redis.Spec.AutoScale.MaxReplicas
		}

		if redis.Spec.AutoScale.MinReplicas != *specNew.MinReplicas {
			specNew.MinReplicas = &redis.Spec.AutoScale.MinReplicas
		}
		if !reflect.DeepEqual(hpa.Spec, *specNew) {
			hpa.Spec = *specNew
			if err = r.Update(ctx, &hpa); err != nil {
				return err
			}
			log.Info("hpa update success")
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return nil, nil
}

func (r *RedisReconciler) updateStatusWithRetry(ctx context.Context, redis *zbn0922v1.Redis) (*ctrl.Result, error) {
	err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		stsName := redis.Name
		key := types.NamespacedName{
			Namespace: redis.Namespace,
			Name:      stsName,
		}
		var sts appsv1.StatefulSet
		if err := r.Get(ctx, key, &sts); err != nil {
			if apierrors.IsNotFound(err) {
				return nil
			}
			return err
		}

		replicas := int32(1)
		if sts.Spec.Replicas != nil {
			replicas = *sts.Spec.Replicas
		}

		newStatus := redis.Status.DeepCopy()
		newStatus.Replicas = replicas
		newStatus.ReadyReplicas = sts.Status.ReadyReplicas
		newStatus.ObservedGeneration = redis.Generation

		if sts.Status.ReadyReplicas == replicas {
			meta.SetStatusCondition(&newStatus.Conditions, metav1.Condition{
				Type:               "Ready",
				Status:             metav1.ConditionTrue,
				Reason:             "StatefulSetReady",
				Message:            "Redis cluster ready",
				ObservedGeneration: redis.GetGeneration(),
				LastTransitionTime: metav1.Now(),
			})
		} else {
			meta.SetStatusCondition(&newStatus.Conditions, metav1.Condition{
				Type:               "Ready",
				Status:             metav1.ConditionFalse,
				Reason:             "PodsNotReady",
				Message:            "Waiting for pods ready",
				ObservedGeneration: redis.GetGeneration(),
				LastTransitionTime: metav1.Now(),
			})
		}

		var pods corev1.PodList
		err := r.List(
			ctx,
			&pods,
			client.InNamespace(redis.Namespace),
			client.MatchingLabels{
				"app": redis.Name,
			},
		)
		if err != nil {
			return err
		}
		master := r.findMater(ctx, pods)
		newStatus.Master = master.Name

		//newStatus.Master = pods.Items[0].Name

		if !reflect.DeepEqual(redis.Status, *newStatus) {
			redis.Status = *newStatus
			if err := r.Status().Update(ctx, redis); err != nil {
				return err
			}
			return nil
		}

		return nil
	})

	if err != nil {
		return nil, err
	}

	return nil, nil
}

func (r *RedisReconciler) execRedisCommand(ctx context.Context, pod corev1.Pod, command []string) (string, string, error) {

	req := r.Clientset.CoreV1().
		RESTClient().
		Post().
		Resource("pods").
		Name(pod.Name).
		Namespace(pod.Namespace).
		SubResource("exec").
		VersionedParams(&corev1.PodExecOptions{
			Container: "redis",
			Command:   command,
			Stdout:    true,
			Stderr:    true,
			Stdin:     true,
			TTY:       false,
		}, scheme.ParameterCodec) // 注意这里的scheme需要是 	"k8s.io/client-go/kubernetes/scheme"

	u := req.URL()
	logf.FromContext(ctx).Info("Exec URL", "url", u.String())
	executor, err := remotecommand.NewSPDYExecutor(
		r.Config,
		"POST",
		req.URL(),
	)
	if err != nil {
		return "", "", err
	}

	var stdout, stderr bytes.Buffer

	err = executor.StreamWithContext(ctx, remotecommand.StreamOptions{
		Stdin:  strings.NewReader(""),
		Stdout: &stdout,
		Stderr: &stderr,
	})

	return stdout.String(), stderr.String(), err
}

// SetupWithManager sets up the controller with the Manager.
func (r *RedisReconciler) SetupWithManager(mgr ctrl.Manager) error {
	//pc := predicate.Funcs{

	//	CreateFunc: func(e event.CreateEvent) bool {
	//		return true
	//	},

	//	UpdateFunc: func(e event.UpdateEvent) bool {
	//		return e.ObjectOld.GetGeneration() != e.ObjectNew.GetGeneration()
	//	},

	//	DeleteFunc: func(e event.DeleteEvent) bool {
	//		return true
	//	},
	//}
	//pc := predicate.GenerationChangedPredicate{}
	return ctrl.NewControllerManagedBy(mgr).
		//For(&zbn0922v1.Redis{}, builder.WithPredicates(predicate.GenerationChangedPredicate{})).
		For(&zbn0922v1.Redis{}).
		Owns(&appsv1.StatefulSet{}).
		Owns(&corev1.Service{}).
		Owns(&autoscalingv2.HorizontalPodAutoscaler{}).
		WithOptions(controller.Options{
			MaxConcurrentReconciles: 5,
		}).
		Named("redis").
		Complete(r)
}
