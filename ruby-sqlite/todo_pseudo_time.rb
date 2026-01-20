require "sequel"
require "logger"
require "prettyprint"
require "securerandom"
require "fileutils"

class TodoPseudotimeDemo
  def initialize
    reset
  end

  def reset
    FileUtils.rm_f("test.db")
    @db = Sequel.sqlite # memory database, requires sqlite3
  end

  def create_tables
    # 2. Create tables
    @db.create_table :worlds do
      primary_key :id
      Integer :pseudotime, default: 0, null: false
    end

    @db.create_table :tasks do
      # IDs and foreign keys
      primary_key :id
      String :eid, null: false

      # domain specific columns
      String :content, size: 100, null: false

      # pseudotime+world housekeeping (soft delete is a must)
      foreign_key :world_id, :worlds, null: false
      Integer :created_at_pseudotime, null: false
      Integer :valid_before_pseudotime, null: true
      TrueClass :is_deleted
    end
  end

  def run
    #1 reset the demo
    reset
    #2 create the schema
    create_tables

    # @db.loggers << Logger.new($stdout) # uncomment to see the real SQL queries

    #3 create a world
    @db[:worlds].insert
    world = World.new(db: @db, id: 1)
    world.report!

    # Insert our first task
    world.transact { |repo| [{table: :tasks, content: "buy bread"}] }
    world.report!

    # Insert our second task
    world.transact { |repo| [{table: :tasks, content: "buy beer"}] }
    world.report!

    # Delete the first task
    world.transact do |repo|
      bread_task = repo.query_tasks { |many| many.where(content: "buy bread") }.first
      [bread_task.merge(is_deleted: true)]
    end
    world.report!

    # Update the second task
    world.transact do |repo|
      beer_task = repo.query_tasks { |many| many.where(content: "buy beer") }.first
      [beer_task.merge(content: "buy whisky")]
    end
    world.report!

    # Access the Past at all the times 0 to 4
    0.upto(4).each do |pseudotime|
      world.report!(pseudotime:)
    end
  end
end

# 3. Define Repository to query the data
class Repository
  def initialize(db:, world_id:, pseudotime:)
    @db = db
    @world_id = world_id
    @pseudotime = pseudotime
  end

  def report!
    puts "*** Snapshot: world:#{@world_id} as-of ptime:#{@pseudotime} **********"
    puts "*** Raw Tables Contents"
    puts "Tasks:"
    table_print(@db[:tasks].all)
    puts "Worlds:"
    table_print(@db[:worlds].all)
    puts
    puts "*** State:"
    pp get_tasks
    puts "********** END State Snapshot"
    puts
    puts
    puts
  end

  def table_print(objs)
    keys = objs.flat_map(&:keys).uniq
    widths = keys.reduce({}) do |ww, k|
      ww.merge(k => (objs.map{|o| o[k].to_s.size } + [k.to_s.size]).max)
    end
    puts keys.map{|k| k.to_s.ljust(widths[k])}.join(' | ')
    objs.each do |o|
      puts(keys.map do |k| 
        o[k].to_s.ljust(widths[k])
      end.join(' | '))
    end

  end

  def get_task(eid)
    many_tasks.where(eid: eid).first
  end

  def get_tasks
    many_tasks.all
  end

  def query_tasks(&blk)
    results = yield many_tasks
    results.map { |r| r.merge(table: :tasks).except(:id) }
  end

  def many_tasks
    @db[:tasks]
      .where(world_id: @world_id)
      .where(created_at_pseudotime: ..@pseudotime)
      .where(Sequel.negate(is_deleted: true))
      .where(Sequel[valid_before_pseudotime: @pseudotime..] | Sequel[valid_before_pseudotime: nil])
  end
end

# 4. Define Worlds to read write within
class World
  def initialize(db:, id:)
    @db = db
    @id = id
  end

  def current_pseudotime
    @db[:worlds].where(id: @id).get(:pseudotime)
  end

  def repository(pseudotime = current_pseudotime)
    Repository.new(db: @db, world_id: @id, pseudotime:)
  end

  def report!(pseudotime: current_pseudotime)
    repository(pseudotime).report!
  end

  def transact(random: SecureRandom, &blk)
    @db.transaction do |db|
      # NOTE: in Postgres/MySQL/etc, we'd need FOR UPDATE here to "lock the world"
      # pseudotime = db.execute("select pseudotime from worlds where id = ?", [@id])
      pseudotime = @db[:worlds].where(id: @id).get(:pseudotime)
      repo = repository(pseudotime)

      new_revision_values = yield repo

      new_revision_values.each do |rev_value|
        if rev_value[:eid].nil?
          rev_value = rev_value.merge(eid: random.uuid)
        end

        table = @db[rev_value[:table]]
        table.where(eid: rev_value[:eid]).where(valid_before_pseudotime: nil).update(
          valid_before_pseudotime: pseudotime
        )

        rev_value[:created_at_pseudotime] = pseudotime + 1
        rev_value[:world_id] = @id
        table.insert(rev_value.except(:table))
      end
      @db[:worlds].where(id: @id).update(pseudotime: pseudotime + 1)
    end
  end
end

TodoPseudotimeDemo.new.run
