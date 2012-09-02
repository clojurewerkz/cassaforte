	private void insertPOISpringTraining() throws Exception {
		//Map<byte[],Map<String,List<Mutation>>>
		Map<byte[], Map<String, List<Mutation>>> outerMap =
			new HashMap<byte[], Map<String, List<Mutation>>>();
		List<Mutation> columnsToAdd = new ArrayList<Mutation>();

		Clock clock = new Clock(System.nanoTime());
		String keyName = "Spring Training";
		Column descCol = new Column("desc".getBytes(UTF8),
			"Fun for baseball fans.".getBytes("UTF-8"), clock);
		Column phoneCol = new Column("phone".getBytes(UTF8),
				"623-333-3333".getBytes(UTF8), clock);

		List<Column> cols = new ArrayList<Column>();
		cols.add(descCol);
		cols.add(phoneCol);

		Map<String, List<Mutation>> innerMap =
			new HashMap<String, List<Mutation>>();

		Mutation columns = new Mutation();
		ColumnOrSuperColumn descCosc = new ColumnOrSuperColumn();
		SuperColumn sc = new SuperColumn();
		sc.name = CAMBRIA_NAME.getBytes();
		sc.columns = cols;

		descCosc.super_column = sc;
		columns.setColumn_or_supercolumn(descCosc);

		columnsToAdd.add(columns);

		String superCFName = "PointOfInterest";
		ColumnPath cp = new ColumnPath();
		cp.column_family = superCFName;
		cp.setSuper_column(CAMBRIA_NAME.getBytes());
		cp.setSuper_columnIsSet(true);

		innerMap.put(superCFName, columnsToAdd);
		outerMap.put(keyName.getBytes(), innerMap);

		client.batch_mutate(outerMap, CL);

		LOG.debug("Done inserting Spring Training.");
	}
