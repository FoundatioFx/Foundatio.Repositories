namespace Foundatio.Repositories {
    public static class CommandOptionsDescriptorExtensions {
        public static ICommandOptions<T> Configure<T>(this CommandOptionsDescriptor<T> configure) where T : class {
            ICommandOptions<T> o = new CommandOptions<T>();
            if (configure != null)
                o = configure(o);

            return o;
        }

        public static ICommandOptions Configure(this CommandOptionsDescriptor configure) {
            ICommandOptions o = new CommandOptions();
            if (configure != null)
                o = configure(o);

            return o;
        }
    }

    public delegate ICommandOptions<T> CommandOptionsDescriptor<T>(ICommandOptions<T> options) where T : class;
    public delegate ICommandOptions CommandOptionsDescriptor(ICommandOptions options);
}
