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
	"context"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	zbn0922v1 "github.com/zbn0922/redis-operator/api/v1"
)

// RedisReconciler reconciles a Redis object
type RedisReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

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
	//log.GetSink().Info()
	log.Info(
		"Redis spec info",
		"namespace", req.Namespace,
		"name", req.Name,
		"replicas", redis.Spec.Replicas, // int32 类型直接传，zap 会自动格式化
		"image", redis.Spec.Image,
		"storage", redis.Spec.Storage,
	)

	// 2、创建hardless service
	var svc corev1.Service
	if err := r.Get(ctx, req.NamespacedName, &svc); err != nil && apierrors.IsNotFound(err) {
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
		if err = ctrl.SetControllerReference(&redis, &newSvc, r.Scheme); err != nil {
			return ctrl.Result{}, err
		}
		// 创建
		if err = r.Create(ctx, &newSvc); err != nil {
			return ctrl.Result{}, err
		}
		// 创建了service后，是异步更新资源，所以需要再次调用下，看下是否创建成功了
		return ctrl.Result{}, nil
	} else if err != nil {
		return ctrl.Result{}, err
	}
	// 3、查看是否存在statefulset
	stsName := redis.Name
	key := types.NamespacedName{
		Namespace: redis.Namespace,
		Name:      stsName,
	}
	var sts appsv1.StatefulSet
	if err := r.Get(ctx, key, &sts); err != nil && apierrors.IsNotFound(err) {
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
		if err = ctrl.SetControllerReference(&redis, &newSts, r.Scheme); err != nil {
			return ctrl.Result{}, err
		}
		if err = r.Create(ctx, &newSts); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{}, nil
	} else if err != nil { //出现错误后重试
		return ctrl.Result{}, err
	}

	// 比对期望和实际状态，如果一致则跳过，否则更新，并添加会workerqueue
	// 从Get获取到的就是实际状态， 自定义cr就是期望值，例如statefulset 中的replicas  redis 是期望， sts是实际状态
	if *sts.Spec.Replicas != redis.Spec.Replicas {
		// 代表需要更新
		sts.Spec.Replicas = &redis.Spec.Replicas

		if err := r.Update(ctx, &sts); err != nil {
			return ctrl.Result{}, err
		}

		return ctrl.Result{}, nil
	}

	// image
	//container := sts.Spec.Template.Spec.Containers[0]
	if sts.Spec.Template.Spec.Containers[0].Image != redis.Spec.Image {
		sts.Spec.Template.Spec.Containers[0].Image = redis.Spec.Image

		if err := r.Update(ctx, &sts); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{}, nil
	}

	// 更新cr status
	if redis.Status.ReadyReplicas != sts.Status.ReadyReplicas || redis.Status.Replicas != *sts.Spec.Replicas {
		redis.Status.Replicas = *sts.Spec.Replicas
		redis.Status.ReadyReplicas = sts.Status.ReadyReplicas
		if sts.Status.ReadyReplicas == *sts.Spec.Replicas {
			redis.Status.Phase = "Running"
		} else {
			redis.Status.Phase = "Pending"
		}
		redis.Status.ObservedGeneration = redis.ObjectMeta.Generation
		if err := r.Status().Update(ctx, &redis); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{}, nil
	}
	// TODO(user): your logic here
	return ctrl.Result{}, nil
}

// getPhase 根据 StatefulSet 状态计算 Redis 的 Phase
func getPhase(sts appsv1.StatefulSet) string {
	if sts.Spec.Replicas != nil && *sts.Spec.Replicas > 0 && sts.Status.ReadyReplicas == *sts.Spec.Replicas {
		return "Running"
	}
	return "Pending"
}

// SetupWithManager sets up the controller with the Manager.
func (r *RedisReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&zbn0922v1.Redis{}).
		Owns(&appsv1.StatefulSet{}).
		Owns(&corev1.Service{}).
		Named("redis").
		Complete(r)
}
