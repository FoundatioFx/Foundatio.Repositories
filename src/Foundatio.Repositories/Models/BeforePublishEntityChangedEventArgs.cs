using System.ComponentModel;

namespace Foundatio.Repositories.Models {
  public class BeforePublishEntityChangedEventArgs<T> : CancelEventArgs where T : class, IIdentity, new() {
        public BeforePublishEntityChangedEventArgs(IRepository<T> repository, EntityChanged message) {
            Repository = repository;
            Message = message;
        }

        public EntityChanged Message { get; private set; }
        public IReadOnlyRepository<T> Repository { get; private set; }
    }
}