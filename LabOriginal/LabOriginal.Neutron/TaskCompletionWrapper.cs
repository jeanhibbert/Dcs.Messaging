using System.Threading.Tasks;

namespace LabOriginal.Neutron
{
    public class TaskCompletionWrapper<T>
    {
        public int Id { get; set; }

        public TaskCompletionSource<T> TaskCompletionSource { get; set; }

        public TaskCompletionWrapper(int id, TaskCompletionSource<T> taskCompletionSource)
        {
            Id = id;
            TaskCompletionSource = taskCompletionSource;
        }
    }
}
