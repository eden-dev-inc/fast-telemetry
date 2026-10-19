#define _GNU_SOURCE
#include <stdint.h>
#include <stddef.h>
#include <stdlib.h>
#include <stdio.h>
#include <stdatomic.h>
#include <signal.h>
#include <ucontext.h>
#include <dlfcn.h>
#include <pthread.h>
#include <sys/syscall.h>
#include <unistd.h>
#include <time.h>

/* Diagnostic profiler only. No runtime code or instrumentation is retained. */
extern void *__libc_malloc(size_t);
extern void *__libc_calloc(size_t,size_t);
extern void *__libc_realloc(void*,size_t);
extern void __libc_free(void*);
#define N 65536
#define P 8
_Static_assert(ATOMIC_POINTER_LOCK_FREE==2 && ATOMIC_LONG_LOCK_FREE==2 && ATOMIC_LLONG_LOCK_FREE==2,"signal sampling requires lock-free atomics");
static _Atomic uintptr_t ips[N];
static _Atomic uint64_t samples[P][N];
static _Atomic uint64_t phase_ns[P], phase_wall[P], phase_calls[P];
static _Atomic uint64_t allocs[P], alloc_bytes[P], frees[P];
static _Atomic uint64_t errors, overflow;
static _Atomic int active;
static _Thread_local volatile sig_atomic_t phase;
static _Thread_local uint64_t beginning;
static _Thread_local uint64_t wall_beginning;
static int allocation_mode;
static int (*real_clock_gettime)(clockid_t,struct timespec*);

static uint64_t cpu_ns(void) {
    struct timespec time;
    if(real_clock_gettime(CLOCK_THREAD_CPUTIME_ID,&time)) {atomic_fetch_add(&errors,1);return 0;}
    return (uint64_t)time.tv_sec*1000000000ULL+(uint64_t)time.tv_nsec;
}
static uint64_t wall_ns(void) {
    struct timespec time;
    if(real_clock_gettime(CLOCK_MONOTONIC,&time)){atomic_fetch_add(&errors,1);return 0;}
    return (uint64_t)time.tv_sec*1000000000ULL+(uint64_t)time.tv_nsec;
}
void trace_profile_window(int enabled) {atomic_store_explicit(&active,enabled,memory_order_release);}
void trace_profile_enter(int next) {
    phase=next;
    beginning=next && atomic_load_explicit(&active,memory_order_acquire)?cpu_ns():0;
    wall_beginning=beginning?wall_ns():0;
}
void trace_profile_exit(void) {
    if(beginning && phase) {
        atomic_fetch_add(&phase_ns[phase],cpu_ns()-beginning);
        atomic_fetch_add(&phase_wall[phase],wall_ns()-wall_beginning);
        atomic_fetch_add(&phase_calls[phase],1);
    }
    phase=0;beginning=0;
}

static int admitted_phase(void) {
    return atomic_load_explicit(&active,memory_order_acquire)?phase:0;
}
static void sampled(int sig,siginfo_t *info,void *context) {
    (void)sig;(void)info;
    int p=admitted_phase();if(!p)return;
    uintptr_t ip=((ucontext_t*)context)->uc_mcontext.gregs[REG_RIP];
    size_t i=(ip>>3)&(N-1);
    for(size_t n=0;n<N;n++,i=(i+1)&(N-1)) {
        uintptr_t previous=0;
        atomic_compare_exchange_strong(&ips[i],&previous,ip);
        if(atomic_load(&ips[i])==ip){atomic_fetch_add(&samples[p][i],1);return;}
    }
    atomic_fetch_add(&overflow,1);
}
static void allocated(size_t bytes) {
    if(!allocation_mode)return;
    int p=admitted_phase();if(!p)return;
    atomic_fetch_add(&allocs[p],1);atomic_fetch_add(&alloc_bytes[p],bytes);
}
void *malloc(size_t bytes){void *ptr=__libc_malloc(bytes);if(ptr)allocated(bytes);return ptr;}
void *calloc(size_t count,size_t bytes){void *ptr=__libc_calloc(count,bytes);if(ptr)allocated(count*bytes);return ptr;}
void *realloc(void *ptr,size_t bytes){void *next=__libc_realloc(ptr,bytes);if(next)allocated(bytes);return next;}
void free(void *ptr){if(ptr && allocation_mode){int p=admitted_phase();if(p)atomic_fetch_add(&frees[p],1);}__libc_free(ptr);}

static timer_t main_timer;
static timer_t start_timer(void) {
    struct sigevent event={0};event.sigev_notify=SIGEV_THREAD_ID;
    event.sigev_signo=SIGPROF;event._sigev_un._tid=syscall(SYS_gettid);
    timer_t timer=0;
    if(timer_create(CLOCK_THREAD_CPUTIME_ID,&event,&timer)){atomic_fetch_add(&errors,1);return 0;}
    struct itimerspec interval={{0,500000},{0,500000}};
    if(timer_settime(timer,0,&interval,0))atomic_fetch_add(&errors,1);
    return timer;
}
struct start_arg {void *(*callback)(void*);void *arg;};
static void *thread_start(void *ptr) {
    struct start_arg data=*(struct start_arg*)ptr;__libc_free(ptr);
    phase=6; /* Uninstrumented SDK background worker. Producers override this. */
    timer_t timer=start_timer();void *result=data.callback(data.arg);timer_delete(timer);return result;
}
int pthread_create(pthread_t *thread,const pthread_attr_t *attributes,void *(*callback)(void*),void *arg) {
    int (*real_create)(pthread_t*,const pthread_attr_t*,void *(*)(void*),void*)=dlsym(RTLD_NEXT,"pthread_create");
    struct start_arg *data=__libc_malloc(sizeof(*data));if(!data)return 12;
    data->callback=callback;data->arg=arg;
    int error=real_create(thread,attributes,thread_start,data);if(error)__libc_free(data);return error;
}
__attribute__((constructor)) static void start(void) {
    real_clock_gettime=dlsym(RTLD_NEXT,"clock_gettime");
    allocation_mode=getenv("TRACE_PROFILE_ALLOCATIONS")!=0;
    struct sigaction action={0};action.sa_sigaction=sampled;action.sa_flags=SA_SIGINFO|SA_RESTART;
    if(sigaction(SIGPROF,&action,0))atomic_fetch_add(&errors,1);
    main_timer=start_timer();
}
__attribute__((destructor)) static void finish(void) {
    atomic_store(&active,0);timer_delete(main_timer);
    fprintf(stderr,"PROFILE_ERRORS %lu OVERFLOW %lu\n",atomic_load(&errors),atomic_load(&overflow));
    for(int p=1;p<P;p++) {
        fprintf(stderr,"PHASE %d %lu %lu %lu\n",p,atomic_load(&phase_calls[p]),atomic_load(&phase_ns[p]),atomic_load(&phase_wall[p]));
        fprintf(stderr,"ALLOC %d %lu %lu %lu\n",p,atomic_load(&allocs[p]),atomic_load(&alloc_bytes[p]),atomic_load(&frees[p]));
        for(size_t i=0;i<N;i++) {
            uint64_t count=atomic_load(&samples[p][i]);if(!count)continue;
            uintptr_t ip=atomic_load(&ips[i]);Dl_info info;
            if(dladdr((void*)ip,&info))fprintf(stderr,"SAMPLE %d %lu %s+0x%lx\n",p,count,info.dli_fname,ip-(uintptr_t)info.dli_fbase);
            else fprintf(stderr,"SAMPLE %d %lu 0x%lx\n",p,count,ip);
        }
    }
}
