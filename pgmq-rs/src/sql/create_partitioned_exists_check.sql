SELECT EXISTS(SELECT * from part_config where parent_table = $1) AS exists;
