### SCons build recipe for CBT

# Important targets:
#
# build     - build the software (default)
# install   - install library
#
# audit     - run code-auditing tools
#
# TODO:
# uninstall - undo an install
# check     - run regression and unit tests.


import os.path

try:
    Import('env')
except:
    exec open("build-env.py")
    env = Environment()

# Utility productions
def Utility(target, source, action):
    target = env.Command(target=target, source=source, action=action)
    env.AlwaysBuild(target)
    env.Precious(target)
    return target

src_files = [Glob('src/*.cpp'), Glob('util/*.cpp'), Glob('common/*.cpp'), Glob('src/*.cu')]
cbt_install_headers = Glob('src/*.h')
prefix = '/usr/local'

env.Append(CCFLAGS = ['-g','-O2','-Wall'])
cbtlib = env.SharedLibrary('gpucbt', src_files,
            CPPFLAGS = ['-Isrc/', '-Iutil/', '-Icommon', '-I/usr/local/cuda/include'],
            LIBS = ['-ljemalloc'])

test_files = ['test/test.pb.cc', 'test/testCBT.cpp']
testapp = env.Program('test/testcbt', test_files,
            LIBS = ['-lgtest', '-lprotobuf', '-lpthread', '-lgpucbt', '-lsnappy'])

client_files = ['service/Client.cpp', env.Object('common/Message.cpp'), env.Object('util/HashUtil.cpp')]
client_app = env.Program('service/gpucbtclient', client_files,
            LIBS = ['-lprotobuf', '-lgpucbt', '-lsnappy', '-lzmq', '-ldl'])

server_files = ['service/Server.cpp', env.Object('common/Message.cpp')]
server_app = env.Program('service/gpucbtserver', server_files,
            LIBS = ['-lprotobuf', '-lgpucbt', '-lsnappy', '-lzmq', '-lpthread', '-lgflags', '-ljemalloc', '-ldl'])

## Targets
# build targets
build = env.Alias('build', [cbtlib])
env.Default(*build)

# install targets
env.Alias('install-lib', Install(os.path.join(prefix, "lib"), cbtlib))
env.Alias('install-headers', Install(os.path.join(prefix, "include", "cbt"), cbt_install_headers))
env.Alias('install', ['install-lib', 'install-headers'])

# audit
Utility("cppcheck", [], "cppcheck --template gcc --enable=all --force src/")
audit = env.Alias('audit', ['cppcheck'])

# test
test = env.Alias('test', [testapp])

#service
test = env.Alias('service', [client_app, server_app])
