namespace Foundatio.Repositories {
    public static class RepositoryQueryDescriptorExtensions {
        public static IRepositoryQuery<T> Configure<T>(this RepositoryQueryDescriptor<T> configure) where T : class {
            IRepositoryQuery<T> o = new RepositoryQuery<T>();
            if (configure != null)
                o = configure(o);

            return o;
        }

        public static IRepositoryQuery Configure(this RepositoryQueryDescriptor configure) {
            IRepositoryQuery o = new RepositoryQuery();
            if (configure != null)
                o = configure(o);

            return o;
        }
    }

    public delegate IRepositoryQuery<T> RepositoryQueryDescriptor<T>(IRepositoryQuery<T> query) where T : class;
    public delegate IRepositoryQuery RepositoryQueryDescriptor(IRepositoryQuery query);
 }
